package smt

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

func IncrementIntermediateHashes(ctx context.Context, logPrefix string, db kv.RwTx, dbSmt *smt.SMT, from, to uint64) (common.Hash, error) {
	log.Info(fmt.Sprintf("[%s] Increment trie hashes started", logPrefix), "previousRootHeight", from, "calculatingRootHeight", to)
	defer log.Info(fmt.Sprintf("[%s] Increment ended", logPrefix))

	changesGetter := NewIncrementChangesGetter(db)
	if err := changesGetter.openChangesGetter(from); err != nil {
		return trie.EmptyRoot, fmt.Errorf("OpenChangesGetter: %w", err)
	}
	defer changesGetter.closeChangesGetter()

	// case when we are incrementing from block 1
	// we chould include the 0 block which is the genesis data
	if from != 0 {
		from += 1
	}

	for i := from; i <= to; i++ {
		select {
		case <-ctx.Done():
			return trie.EmptyRoot, fmt.Errorf("context done")
		default:
		}
		if err := changesGetter.getChangesForBlock(i); err != nil {
			return trie.EmptyRoot, fmt.Errorf("getChangesForBlock: %w", err)
		}
	}

	if _, _, err := dbSmt.SetStorage(
		ctx,
		logPrefix,
		changesGetter.accChanges,
		changesGetter.codeChanges,
		changesGetter.storageChanges,
	); err != nil {
		return trie.EmptyRoot, err
	}

	log.Info(fmt.Sprintf("[%s] Increment trie hashes finished. Commiting batch", logPrefix))

	// do not put this outside, because sequencer uses this function to calculate root for each block
	hermezDb := hermez_db.NewHermezDb(db)
	if err := hermezDb.WriteSmtDepth(to, uint64(dbSmt.GetDepth())); err != nil {
		return trie.EmptyRoot, err
	}

	return common.BigToHash(dbSmt.LastRoot()), nil
}
