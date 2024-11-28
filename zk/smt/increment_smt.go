package smt

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	eridb "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/status-im/keycard-go/hexutils"
)

func IncrementIntermediateHashes(ctx context.Context, logPrefix string, s *stagedsync.StageState, db kv.RwTx, eridb *eridb.EriDb, dbSmt *smt.SMT, from, to uint64) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Increment trie hashes started", logPrefix), "previousRootHeight", s.BlockNumber, "calculatingRootHeight", to)
	defer log.Info(fmt.Sprintf("[%s] Increment ended", logPrefix))

	ac, err := db.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer ac.Close()

	sc, err := db.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return trie.EmptyRoot, err
	}
	defer sc.Close()

	// progress printer
	accChanges := make(map[common.Address]*accounts.Account)
	codeChanges := make(map[common.Address]string)
	storageChanges := make(map[common.Address]map[string]string)

	// case when we are incrementing from block 1
	// we chould include the 0 block which is the genesis data
	if from != 0 {
		from += 1
	}

	// NB: changeset tables are zero indexed
	// changeset tables contain historical value at N-1, so we look up values from plainstate
	// i+1 to get state at the beginning of the next batch
	psr := state.NewPlainState(db, from+1, systemcontracts.SystemContractCodeLookup["Hermez"])
	defer psr.Close()

	for i := from; i <= to; i++ {
		dupSortKey := dbutils.EncodeBlockNumber(i)
		psr.SetBlockNr(i + 1)

		// collect changes to accounts and code
		for _, v, err := ac.SeekExact(dupSortKey); err == nil && v != nil; _, v, err = ac.NextDup() {
			addr := common.BytesToAddress(v[:length.Addr])

			currAcc, err := psr.ReadAccountData(addr)
			if err != nil {
				return trie.EmptyRoot, err
			}

			// store the account
			accChanges[addr] = currAcc

			cc, err := psr.ReadAccountCode(addr, currAcc.Incarnation, currAcc.CodeHash)
			if err != nil {
				return trie.EmptyRoot, err
			}

			ach := hexutils.BytesToHex(cc)
			if len(ach) > 0 {
				hexcc := "0x" + ach
				codeChanges[addr] = hexcc
			}
		}

		err = db.ForPrefix(kv.StorageChangeSet, dupSortKey, func(sk, sv []byte) error {
			changesetKey := sk[length.BlockNum:]
			address, incarnation := dbutils.PlainParseStoragePrefix(changesetKey)

			sstorageKey := sv[:length.Hash]
			stk := common.BytesToHash(sstorageKey)

			value, err := psr.ReadAccountStorage(address, incarnation, &stk)
			if err != nil {
				return err
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
	}

	if _, _, err := dbSmt.SetStorage(ctx, logPrefix, accChanges, codeChanges, storageChanges); err != nil {
		return trie.EmptyRoot, err
	}

	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes finished. Commiting batch", logPrefix))

	lr := dbSmt.LastRoot()

	hash := common.BigToHash(lr)

	// do not put this outside, because sequencer uses this function to calculate root for each block
	hermezDb := hermez_db.NewHermezDb(db)
	if err := hermezDb.WriteSmtDepth(to, uint64(dbSmt.GetDepth())); err != nil {
		return trie.EmptyRoot, err
	}

	return hash, nil
}
