package smt

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/status-im/keycard-go/hexutils"
)

func UnwindZkSMT(ctx context.Context, logPrefix string, from, to uint64, tx kv.RwTx, checkRoot bool, expectedRootHash *common.Hash, quiet bool) (common.Hash, error) {
	if !quiet {
		log.Info(fmt.Sprintf("[%s] Unwind trie hashes started", logPrefix))
		defer log.Info(fmt.Sprintf("[%s] Unwind ended", logPrefix))
	}

	eridb := db2.NewEriDb(tx)
	eridb.RollbackBatch()

	dbSmt := smt.NewSMT(eridb, false)

	if !quiet {
		log.Info(fmt.Sprintf("[%s]", logPrefix), "last root", common.BigToHash(dbSmt.LastRoot()))
	}

	// only open the batch if tx is not already one
	if _, ok := tx.(*membatchwithdb.MemoryMutation); !ok {
		quit := make(chan struct{})
		eridb.OpenBatch(quit)
	}

	changesGetter := NewChangesGetter(tx)
	if err := changesGetter.openChangesGetter(from); err != nil {
		return trie.EmptyRoot, fmt.Errorf("OpenChangesGetter: %w", err)
	}
	defer changesGetter.closeChangesGetter()

	total := uint64(math.Abs(float64(from) - float64(to) + 1))
	progressChan, stopPrinter := zk.ProgressPrinter(fmt.Sprintf("[%s] Progress unwinding", logPrefix), total, quiet)
	defer stopPrinter()

	// walk backwards through the blocks, applying state changes, and deletes
	// PlainState contains data AT the block
	// History tables contain data BEFORE the block - so need a +1 offset
	for i := from; i >= to+1; i-- {
		select {
		case <-ctx.Done():
			return trie.EmptyRoot, fmt.Errorf("context done")
		default:
		}

		if err := changesGetter.getChangesForBlock(i); err != nil {
			return trie.EmptyRoot, fmt.Errorf("getChangesForBlock: %w", err)
		}

		progressChan <- 1
	}

	stopPrinter()

	if _, _, err := dbSmt.SetStorage(ctx, logPrefix, changesGetter.accChanges, changesGetter.codeChanges, changesGetter.storageChanges); err != nil {
		return trie.EmptyRoot, err
	}

	lr := dbSmt.LastRoot()

	hash := common.BigToHash(lr)
	if checkRoot && hash != *expectedRootHash {
		log.Error("failed to verify hash")
		return trie.EmptyRoot, fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
	}

	if !quiet {
		log.Info(fmt.Sprintf("[%s] Trie root matches", logPrefix), "hash", hash.Hex())
	}

	if err := eridb.CommitBatch(); err != nil {
		return trie.EmptyRoot, err
	}

	return hash, nil
}

var (
	ErrAlreadyOpened = errors.New("already opened")
	ErrNotOpened     = errors.New("not opened")
)

type changesGetter struct {
	tx kv.Tx

	ac         kv.CursorDupSort
	sc         kv.CursorDupSort
	psr        *state.PlainState
	currentPsr *state.PlainStateReader

	accChanges     map[common.Address]*accounts.Account
	codeChanges    map[common.Address]string
	storageChanges map[common.Address]map[string]string

	opened bool
}

func NewChangesGetter(tx kv.Tx) *changesGetter {
	return &changesGetter{
		tx:             tx,
		accChanges:     make(map[common.Address]*accounts.Account),
		codeChanges:    make(map[common.Address]string),
		storageChanges: make(map[common.Address]map[string]string),
	}
}
func (cg *changesGetter) addDeletedAcc(addr common.Address) {
	deletedAcc := new(accounts.Account)
	deletedAcc.Balance = *uint256.NewInt(0)
	deletedAcc.Nonce = 0
	cg.accChanges[addr] = deletedAcc
}

func (cg *changesGetter) openChangesGetter(from uint64) error {
	if cg.opened {
		return ErrAlreadyOpened
	}

	ac, err := cg.tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return fmt.Errorf("CursorDupSort: %w", err)
	}

	sc, err := cg.tx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return fmt.Errorf("CursorDupSort: %w", err)
	}

	cg.ac = ac
	cg.sc = sc
	cg.psr = state.NewPlainState(cg.tx, from, systemcontracts.SystemContractCodeLookup["Hermez"])
	cg.currentPsr = state.NewPlainStateReader(cg.tx)

	cg.opened = true

	return nil
}

func (cg *changesGetter) closeChangesGetter() {
	if cg.ac != nil {
		cg.ac.Close()
	}

	if cg.sc != nil {
		cg.sc.Close()
	}

	if cg.psr != nil {
		cg.psr.Close()
	}
}

func (cg *changesGetter) getChangesForBlock(blockNum uint64) error {
	if !cg.opened {
		return ErrNotOpened
	}

	cg.psr.SetBlockNr(blockNum)
	dupSortKey := dbutils.EncodeBlockNumber(blockNum)

	// collect changes to accounts and code
	for _, v, err2 := cg.ac.SeekExact(dupSortKey); err2 == nil && v != nil; _, v, err2 = cg.ac.NextDup() {
		if err := cg.setAccountChangesFromV(v); err != nil {
			return fmt.Errorf("failed to get account changes: %w", err)
		}
	}

	if err := cg.tx.ForPrefix(kv.StorageChangeSet, dupSortKey, cg.setStorageChangesFromKv); err != nil {
		return fmt.Errorf("failed to get storage changes: %w", err)
	}

	return nil
}

func (cg *changesGetter) setAccountChangesFromV(v []byte) error {
	addr := common.BytesToAddress(v[:length.Addr])

	// if the account was created in this changeset we should delete it
	if len(v[length.Addr:]) == 0 {
		cg.codeChanges[addr] = ""
		cg.addDeletedAcc(addr)
		return nil
	}

	oldAcc, err := cg.psr.ReadAccountData(addr)
	if err != nil {
		return fmt.Errorf("ReadAccountData: %w", err)
	}

	// currAcc at block we're unwinding from
	currAcc, err := cg.currentPsr.ReadAccountData(addr)
	if err != nil {
		return fmt.Errorf("ReadAccountData: %w", err)
	}

	if oldAcc.Incarnation > 0 {
		if len(v) == 0 { // self-destructed
			cg.addDeletedAcc(addr)
		} else {
			if currAcc.Incarnation > oldAcc.Incarnation {
				cg.addDeletedAcc(addr)
			}
		}
	}

	// store the account
	cg.accChanges[addr] = oldAcc

	if oldAcc.CodeHash != currAcc.CodeHash {
		hexcc, err := cg.getCodehashChanges(addr, oldAcc)
		if err != nil {
			return fmt.Errorf("getCodehashChanges: %w", err)
		}
		cg.codeChanges[addr] = hexcc
	}

	return nil
}

func (cg *changesGetter) getCodehashChanges(addr common.Address, oldAcc *accounts.Account) (string, error) {
	cc, err := cg.currentPsr.ReadAccountCode(addr, oldAcc.Incarnation, oldAcc.CodeHash)
	if err != nil {
		return "", fmt.Errorf("ReadAccountCode: %w", err)
	}

	ach := hexutils.BytesToHex(cc)
	hexcc := ""
	if len(ach) > 0 {
		hexcc = "0x" + ach
	}

	return hexcc, nil
}

func (cg *changesGetter) setStorageChangesFromKv(sk, sv []byte) error {
	changesetKey := sk[length.BlockNum:]
	address, _ := dbutils.PlainParseStoragePrefix(changesetKey)

	sstorageKey := sv[:length.Hash]
	stk := common.BytesToHash(sstorageKey)

	value := []byte{0}
	if len(sv[length.Hash:]) != 0 {
		value = sv[length.Hash:]
	}

	stkk := fmt.Sprintf("0x%032x", stk)
	v := fmt.Sprintf("0x%032x", common.BytesToHash(value))

	m := make(map[string]string)
	m[stkk] = v

	if cg.storageChanges[address] == nil {
		cg.storageChanges[address] = make(map[string]string)
	}
	cg.storageChanges[address][stkk] = v

	return nil
}
