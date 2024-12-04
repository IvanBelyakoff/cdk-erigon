package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

type blockExecutor struct {
	// cfg          ExecuteBlockCfg
	tx                   kv.RwTx
	batch                kv.StatelessRwTx
	blockReader          services.FullBlockReader
	accumulator          *shards.Accumulator
	engine               consensus.Engine
	vmConfig             vm.Config
	chainConfig          *chain.Config
	roHermezDb           state.ReadOnlyHermezDb
	stateStream          bool
	initialCycle         bool
	changeSetHook        ChangeSetHook
	nextStagesExpectData bool
	hostoryPruneTo       uint64
	receiptsPruneTo      uint64
	callTracesPruneTo    uint64

	prevBlockRoot common.Hash
}

func (be *blockExecutor) executeBlock(
	block *types.Block,
) (execRs *core.EphemeralExecResultZk, err error) {
	blockNum := block.NumberU64()
	// Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet
	writeChangeSets := be.nextStagesExpectData || blockNum > be.hostoryPruneTo
	writeReceipts := be.nextStagesExpectData || blockNum > be.receiptsPruneTo
	writeCallTraces := be.nextStagesExpectData || blockNum > be.callTracesPruneTo

	stateReader, stateWriter, err := newStateReaderWriter(be.batch, be.tx, block, writeChangeSets, be.accumulator, be.blockReader, be.stateStream)
	if err != nil {
		return nil, fmt.Errorf("newStateReaderWriter: %w", err)
	}

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := be.blockReader.Header(context.Background(), be.tx, hash, number)
		return h
	}

	getTracer := func(txIndex int, txHash common.Hash) (vm.EVMLogger, error) {
		// return logger.NewJSONFileLogger(&logger.LogConfig{}, txHash.String()), nil
		return logger.NewStructLogger(&logger.LogConfig{}), nil
	}

	callTracer := calltracer.NewCallTracer()
	be.vmConfig.Debug = true
	be.vmConfig.Tracer = callTracer

	getHashFn := core.GetHashFn(block.Header(), getHeader)
	if execRs, err = core.ExecuteBlockEphemerallyZk(be.chainConfig, &be.vmConfig, getHashFn, be.engine, block, stateReader, stateWriter, ChainReaderImpl{config: be.chainConfig, tx: be.tx, blockReader: be.blockReader}, getTracer, be.roHermezDb, &be.prevBlockRoot); err != nil {
		return nil, fmt.Errorf("ExecuteBlockEphemerallyZk: %w", err)
	}

	if writeReceipts {
		if err := rawdb.AppendReceipts(be.tx, blockNum, execRs.Receipts); err != nil {
			return nil, fmt.Errorf("AppendReceipts: %w", err)
		}

		stateSyncReceipt := execRs.StateSyncReceipt
		if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusSuccessful {
			if err := rawdb.WriteBorReceipt(be.tx, block.NumberU64(), stateSyncReceipt); err != nil {
				return nil, fmt.Errorf("WriteBorReceipt: %w", err)
			}
		}
	}

	if be.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			be.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}
	if writeCallTraces {
		if err := callTracer.WriteToDb(be.tx, block, be.vmConfig); err != nil {
			return nil, fmt.Errorf("WriteToDb: %w", err)
		}
	}

	be.prevBlockRoot = block.Root()
	return execRs, nil
}
