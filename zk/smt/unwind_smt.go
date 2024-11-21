package smt

import (
	"context"
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
	dbSmt := smt.NewSMT(eridb, false)

	if !quiet {
		log.Info(fmt.Sprintf("[%s]", logPrefix), "last root", common.BigToHash(dbSmt.LastRoot()))
	}

	// only open the batch if tx is not already one
	if _, ok := tx.(*membatchwithdb.MemoryMutation); !ok {
		eridb.OpenBatch(ctx.Done())
	}

	ac, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer ac.Close()

	sc, err := tx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer sc.Close()

	currentPsr := state.NewPlainStateReader(tx)

	total := uint64(math.Abs(float64(from) - float64(to) + 1))
	printerStopped := false
	progressChan, stopPrinter := zk.ProgressPrinter(fmt.Sprintf("[%s] Progress unwinding", logPrefix), total, quiet)
	defer func() {
		if !printerStopped {
			stopPrinter()
		}
	}()

	// walk backwards through the blocks, applying state changes, and deletes
	// PlainState contains data AT the block
	// History tables contain data BEFORE the block - so need a +1 offset
	accChanges := make(map[common.Address]*accounts.Account)
	codeChanges := make(map[common.Address]string)
	storageChanges := make(map[common.Address]map[string]string)

	addDeletedAcc := func(addr common.Address) {
		deletedAcc := new(accounts.Account)
		deletedAcc.Balance = *uint256.NewInt(0)
		deletedAcc.Nonce = 0
		accChanges[addr] = deletedAcc
	}

	psr := state.NewPlainState(tx, from, systemcontracts.SystemContractCodeLookup["Hermez"])
	defer psr.Close()

	for i := from; i >= to+1; i-- {
		select {
		case <-ctx.Done():
			return trie.EmptyRoot, fmt.Errorf("[%s] Context done", logPrefix)
		default:
		}

		psr.SetBlockNr(i)

		dupSortKey := dbutils.EncodeBlockNumber(i)

		// collect changes to accounts and code
		for _, v, err2 := ac.SeekExact(dupSortKey); err2 == nil && v != nil; _, v, err2 = ac.NextDup() {

			addr := common.BytesToAddress(v[:length.Addr])

			// if the account was created in this changeset we should delete it
			if len(v[length.Addr:]) == 0 {
				codeChanges[addr] = ""
				addDeletedAcc(addr)
				continue
			}

			oldAcc, err := psr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			// currAcc at block we're unwinding from
			currAcc, err := currentPsr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			if oldAcc.Incarnation > 0 {
				if len(v) == 0 { // self-destructed
					addDeletedAcc(addr)
				} else {
					if currAcc.Incarnation > oldAcc.Incarnation {
						addDeletedAcc(addr)
					}
				}
			}

			// store the account
			accChanges[addr] = oldAcc

			if oldAcc.CodeHash != currAcc.CodeHash {
				cc, err := currentPsr.ReadAccountCode(addr, oldAcc.Incarnation, oldAcc.CodeHash)
				if err != nil {
					return trie.EmptyRoot, err
				}

				ach := hexutils.BytesToHex(cc)
				hexcc := ""
				if len(ach) > 0 {
					hexcc = "0x" + ach
				}
				codeChanges[addr] = hexcc
			}
		}

		err = tx.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
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

			if storageChanges[address] == nil {
				storageChanges[address] = make(map[string]string)
			}
			storageChanges[address][stkk] = v
			return nil
		})
		if err != nil {
			return trie.EmptyRoot, err
		}

		progressChan <- 1
	}

	stopPrinter()
	printerStopped = true

	if _, _, err := dbSmt.SetStorage(ctx, logPrefix, accChanges, codeChanges, storageChanges); err != nil {
		return trie.EmptyRoot, err
	}

	if err := verifyLastHash(dbSmt, expectedRootHash, checkRoot, logPrefix, quiet); err != nil {
		log.Error("failed to verify hash")
		eridb.RollbackBatch()
		return trie.EmptyRoot, err
	}

	if err := eridb.CommitBatch(); err != nil {
		return trie.EmptyRoot, err
	}

	lr := dbSmt.LastRoot()

	hash := common.BigToHash(lr)
	return hash, nil
}

func verifyLastHash(dbSmt *smt.SMT, expectedRootHash *common.Hash, checkRoot bool, logPrefix string, quiet bool) error {
	hash := common.BigToHash(dbSmt.LastRoot())

	if checkRoot && hash != *expectedRootHash {
		return fmt.Errorf("wrong trie root: %x, expected (from header): %x", hash, expectedRootHash)
	}
	if !quiet {
		log.Info(fmt.Sprintf("[%s] Trie root matches", logPrefix), "hash", hash.Hex())
	}
	return nil
}
