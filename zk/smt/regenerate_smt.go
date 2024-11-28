package smt

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/dbutils"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/zk"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

func RegenerateIntermediateHashes(ctx context.Context, logPrefix string, db kv.RwTx, eridb *db2.EriDb, smtIn *smt.SMT, toBlock uint64) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Regeneration trie hashes started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Regeneration ended", logPrefix))

	if err := stages.SaveStageProgress(db, stages.IntermediateHashes, 0); err != nil {
		log.Warn(fmt.Sprint("regenerate SaveStageProgress to zero error: ", err))
	}

	var a *accounts.Account
	var addr common.Address
	var as map[string]string
	var inc uint64

	psr := state.NewPlainStateReader(db)

	accCollector := NewAccountCollector(eridb, psr)

	log.Info(fmt.Sprintf("[%s] Collecting account data...", logPrefix))
	dataCollectStartTime := time.Now()

	// get total accounts count for progress printer
	total := uint64(0)
	if err := psr.ForEach(kv.PlainState, nil, func(k, acc []byte) error {
		total++
		return nil
	}); err != nil {
		return trie.EmptyRoot, err
	}

	progressChan, stopProgressPrinter := zk.ProgressPrinterWithoutValues(fmt.Sprintf("[%s] SMT regenerate progress", logPrefix), total*2)
	defer stopProgressPrinter()

	progCt := uint64(0)
	err := psr.ForEach(kv.PlainState, nil, func(k, acc []byte) error {
		progCt++
		progressChan <- progCt
		var err error
		if len(k) == 20 {
			if a != nil { // don't run process on first loop for first account (or it will miss collecting storage)
				if err = accCollector.processAccount(a, as, inc, addr); err != nil {
					return err
				}
			}

			a = &accounts.Account{}

			if err := a.DecodeForStorage(acc); err != nil {
				// TODO: not an account?
				as = make(map[string]string)
				return nil
			}
			addr = common.BytesToAddress(k)
			inc = a.Incarnation
			// empty storage of previous account
			as = make(map[string]string)
		} else { // otherwise we're reading storage
			_, incarnation, key := dbutils.PlainParseCompositeStorageKey(k)
			if incarnation != inc {
				return nil
			}

			sk := fmt.Sprintf("0x%032x", key)
			v := fmt.Sprintf("0x%032x", acc)

			as[sk] = trimHexString(v)
		}
		return nil
	})

	stopProgressPrinter()

	if err != nil {
		return trie.EmptyRoot, err
	}

	// process the final account
	if err := accCollector.processAccount(a, as, inc, addr); err != nil {
		return trie.EmptyRoot, err
	}

	dataCollectTime := time.Since(dataCollectStartTime)
	log.Info(fmt.Sprintf("[%s] Collecting account data finished in %v", logPrefix, dataCollectTime))

	// generate tree
	if _, err := smtIn.GenerateFromKVBulk(ctx, logPrefix, accCollector.keys); err != nil {
		return trie.EmptyRoot, err
	}

	err2 := db.ClearBucket("HermezSmtAccountValues")
	if err2 != nil {
		log.Warn(fmt.Sprint("regenerate SaveStageProgress to zero error: ", err2))
	}

	root := smtIn.LastRoot()

	// save it here so we don't
	hermezDb := hermez_db.NewHermezDb(db)
	if err := hermezDb.WriteSmtDepth(toBlock, uint64(smtIn.GetDepth())); err != nil {
		return trie.EmptyRoot, err
	}

	return common.BigToHash(root), nil
}

func trimHexString(s string) string {
	s = strings.TrimPrefix(s, "0x")

	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return "0x" + s[i:]
		}
	}

	return "0x0"
}
