package stages

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"

	"context"

	"os"

	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/trie"
	zkSmt "github.com/ledgerwatch/erigon/zk/smt"
)

type ZkInterHashesCfg struct {
	db                kv.RwDB
	checkRoot         bool
	badBlockHalt      bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader
	hd                *headerdownload.HeaderDownload

	historyV3 bool
	agg       *state.Aggregator
	zk        *ethconfig.Zk
}

func StageZkInterHashesCfg(
	db kv.RwDB,
	checkRoot, saveNewHashesToDB, badBlockHalt bool,
	tmpDir string,
	blockReader services.FullBlockReader,
	hd *headerdownload.HeaderDownload,
	historyV3 bool,
	agg *state.Aggregator,
	zk *ethconfig.Zk,
) ZkInterHashesCfg {
	return ZkInterHashesCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,

		historyV3: historyV3,
		agg:       agg,
		zk:        zk,
	}
}

func SpawnZkIntermediateHashesStage(s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, cfg ZkInterHashesCfg, ctx context.Context) (root common.Hash, err error) {
	logPrefix := s.LogPrefix()

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return trie.EmptyRoot, err
		}
		defer tx.Rollback()
	}

	to, err := s.ExecutionAt(tx)
	if err != nil {
		return trie.EmptyRoot, err
	}

	///// DEBUG BISECT /////
	defer func() {
		if cfg.zk.DebugLimit > 0 {
			log.Info(fmt.Sprintf("[%s] Debug limits", logPrefix), "Limit", cfg.zk.DebugLimit, "TO", to, "Err is nil ?", err == nil)
			if err != nil {
				log.Error("Hashing Failed", "block", to, "err", err)
				os.Exit(1)
			}
		}
	}()
	///////////////////////

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return trie.EmptyRoot, nil
	}

	if to > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Generating intermediate hashes", logPrefix), "from", s.BlockNumber, "to", to)
	}

	shouldRegenerate := to > s.BlockNumber && to-s.BlockNumber > cfg.zk.RebuildTreeAfter
	shouldIncrementBecauseOfAFlag := cfg.zk.IncrementTreeAlways
	shouldIncrementBecauseOfExecutionConditions := s.BlockNumber > 0 && !shouldRegenerate
	shouldIncrement := shouldIncrementBecauseOfAFlag || shouldIncrementBecauseOfExecutionConditions

	eridb := db2.NewEriDb(tx)
	smt := smt.NewSMT(eridb, false)

	if cfg.zk.SmtRegenerateInMemory {
		log.Info(fmt.Sprintf("[%s] SMT using mapmutation", logPrefix))
		eridb.OpenBatch(ctx.Done())
	} else {
		log.Info(fmt.Sprintf("[%s] SMT not using mapmutation", logPrefix))
	}

	if shouldIncrement {
		if shouldIncrementBecauseOfAFlag {
			log.Debug(fmt.Sprintf("[%s] IncrementTreeAlways true - incrementing tree", logPrefix), "previousRootHeight", s.BlockNumber, "calculatingRootHeight", to)
		}
		root, err = zkSmt.IncrementIntermediateHashes(ctx, logPrefix, tx, smt, s.BlockNumber, to)
	} else {
		root, err = zkSmt.RegenerateIntermediateHashes(ctx, logPrefix, tx, eridb, smt, to)
	}
	if err != nil {
		return trie.EmptyRoot, err
	}

	log.Info(fmt.Sprintf("[%s] Trie root", logPrefix), "hash", root.Hex())
	if cfg.checkRoot {
		var syncHeadHeader *types.Header
		if syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, tx, to); err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", to)
		}

		expectedRootHash := syncHeadHeader.Root
		headerHash := syncHeadHeader.Hash()
		if root != expectedRootHash {
			if cfg.zk.SmtRegenerateInMemory {
				eridb.RollbackBatch()
			}
			panic(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", logPrefix, to, root, expectedRootHash, headerHash))
		}

		log.Info(fmt.Sprintf("[%s] State root matches", logPrefix))
	}

	if cfg.zk.SmtRegenerateInMemory {
		if err := eridb.CommitBatch(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	if err = s.Update(tx, to); err != nil {
		return trie.EmptyRoot, err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}

	return root, err
}

func UnwindZkIntermediateHashesStage(u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, cfg ZkInterHashesCfg, ctx context.Context, silent bool) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if !silent {
		log.Debug(fmt.Sprintf("[%s] Unwinding intermediate hashes", s.LogPrefix()), "from", s.BlockNumber, "to", u.UnwindPoint)
	}

	syncHeadHeader := rawdb.ReadHeaderByNumber(tx, u.UnwindPoint)
	var expectedRootHash common.Hash
	if syncHeadHeader == nil {
		log.Warn("header not found for block number", "block", u.UnwindPoint)
	} else {
		expectedRootHash = syncHeadHeader.Root
	}

	if _, err = zkSmt.UnwindZkSMT(ctx, s.LogPrefix(), s.BlockNumber, u.UnwindPoint, tx, cfg.checkRoot, &expectedRootHash, silent); err != nil {
		return err
	}

	hermezDb := hermez_db.NewHermezDb(tx)
	if err := hermezDb.TruncateSmtDepths(u.UnwindPoint); err != nil {
		return err
	}

	if err := u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
