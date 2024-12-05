package stagedsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	rawdbZk "github.com/ledgerwatch/erigon/zk/rawdb"
	"github.com/ledgerwatch/log/v3"
)

var (
	ErrExecutionError = fmt.Errorf("execution error")
)

type hermezDb interface {
	WriteBlockInfoRoot(blockNum uint64, root common.Hash) error
	SetNewTx(tx kv.RwTx)
	state.ReadOnlyHermezDb
}

type blockExecutor struct {
	ctx                  context.Context
	logPrefix            string
	cfg                  ExecuteBlockCfg
	tx                   kv.RwTx
	batch                kv.StatelessRwTx
	initialCycle         bool
	nextStagesExpectData bool

	// set internelly
	hermezDb    hermezDb
	stateStream bool

	// these change on each block
	prevBlockRoot       common.Hash
	prevBlockHash       common.Hash
	datastreamBlockHash common.Hash
	block               *types.Block
	currentStateGas     uint64
}

func NewBlockExecutor(
	ctx context.Context,
	logPrefix string,
	cfg ExecuteBlockCfg,
	tx kv.RwTx,
	batch kv.StatelessRwTx,
	initialCycle bool,
	nextStagesExpectData bool,
) *blockExecutor {
	return &blockExecutor{
		ctx:                  ctx,
		logPrefix:            logPrefix,
		cfg:                  cfg,
		tx:                   tx,
		batch:                batch,
		initialCycle:         initialCycle,
		nextStagesExpectData: nextStagesExpectData,
		hermezDb:             hermez_db.NewHermezDb(tx),
	}
}

func (be *blockExecutor) Innit(from, to uint64) (err error) {
	be.stateStream = !be.initialCycle && be.cfg.stateStream && to-from < stateStreamLimit

	be.prevBlockRoot, be.prevBlockHash, err = be.getBlockHashValues(from)
	if err != nil {
		return fmt.Errorf("getBlockHashValues: %w", err)
	}

	return nil
}

func (be *blockExecutor) SetNewTx(tx kv.RwTx, batch kv.StatelessRwTx) {
	be.tx = tx
	be.batch = batch

	be.hermezDb = hermez_db.NewHermezDb(tx)
}

func (be *blockExecutor) ExecuteBlock(blockNum, to uint64) error {
	//fetch values pre execute
	datastreamBlockHash, block, senders, err := be.getPreexecuteValues(blockNum)
	if err != nil {
		return fmt.Errorf("getPreexecuteValues: %w", err)
	}
	be.datastreamBlockHash = datastreamBlockHash
	be.block = block

	execRs, err := be.executeBlock(block, to)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Warn(fmt.Sprintf("[%s] Execution failed", be.logPrefix), "block", blockNum, "hash", be.datastreamBlockHash.Hex(), "err", err)
			if be.cfg.hd != nil {
				be.cfg.hd.ReportBadHeaderPoS(be.datastreamBlockHash, block.ParentHash())
			}
			if be.cfg.badBlockHalt {
				return fmt.Errorf("executeBlockZk: %w", err)
			}
		}
		return fmt.Errorf("%w: %w", ErrExecutionError, err)
	}

	if execRs.BlockInfoTree != nil {
		if err = be.hermezDb.WriteBlockInfoRoot(blockNum, *execRs.BlockInfoTree); err != nil {
			return fmt.Errorf("WriteBlockInfoRoot: %w", err)
		}
	}

	// exec loop variables
	header := block.HeaderNoCopy()
	header.GasUsed = uint64(execRs.GasUsed)
	header.ReceiptHash = types.DeriveSha(execRs.Receipts)
	header.Bloom = execRs.Bloom
	// do not move bove the header setting - hash will differ
	be.prevBlockRoot = block.Root()
	be.prevBlockHash = header.Hash()
	be.currentStateGas = be.currentStateGas + header.GasUsed

	//commit values post execute
	if err := be.postExecuteCommitValues(block, senders); err != nil {
		return fmt.Errorf("postExecuteCommitValues: %w", err)
	}

	return nil
}

func (be *blockExecutor) getBlockHashValues(number uint64) (common.Hash, common.Hash, error) {
	prevheaderHash, err := rawdb.ReadCanonicalHash(be.tx, number)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	header, err := be.cfg.blockReader.Header(be.ctx, be.tx, prevheaderHash, number)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}

	return header.Root, prevheaderHash, nil
}

func (be *blockExecutor) executeBlock(block *types.Block, to uint64) (execRs *core.EphemeralExecResultZk, err error) {
	blockNum := block.NumberU64()

	// Incremental move of next stages depend on fully written ChangeSets, Receipts, CallTraceSet
	writeChangeSets := be.nextStagesExpectData || blockNum > be.cfg.prune.History.PruneTo(to)
	writeReceipts := be.nextStagesExpectData || blockNum > be.cfg.prune.Receipts.PruneTo(to)
	writeCallTraces := be.nextStagesExpectData || blockNum > be.cfg.prune.CallTraces.PruneTo(to)

	stateReader, stateWriter, err := newStateReaderWriter(be.batch, be.tx, block, writeChangeSets, be.cfg.accumulator, be.cfg.blockReader, be.stateStream)
	if err != nil {
		return nil, fmt.Errorf("newStateReaderWriter: %w", err)
	}

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := be.cfg.blockReader.Header(context.Background(), be.tx, hash, number)
		return h
	}

	getTracer := func(txIndex int, txHash common.Hash) (vm.EVMLogger, error) {
		// return logger.NewJSONFileLogger(&logger.LogConfig{}, txHash.String()), nil
		return logger.NewStructLogger(&logger.LogConfig{}), nil
	}

	callTracer := calltracer.NewCallTracer()
	be.cfg.vmConfig.Debug = true
	be.cfg.vmConfig.Tracer = callTracer

	getHashFn := core.GetHashFn(block.Header(), getHeader)
	if execRs, err = core.ExecuteBlockEphemerallyZk(
		be.cfg.chainConfig,
		be.cfg.vmConfig,
		getHashFn,
		be.cfg.engine,
		block,
		stateReader,
		stateWriter,
		ChainReaderImpl{
			config:      be.cfg.chainConfig,
			tx:          be.tx,
			blockReader: be.cfg.blockReader,
		},
		getTracer,
		be.hermezDb,
		&be.prevBlockRoot,
	); err != nil {
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

	if be.cfg.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			be.cfg.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}
	if writeCallTraces {
		if err := callTracer.WriteToDb(be.tx, block, *be.cfg.vmConfig); err != nil {
			return nil, fmt.Errorf("WriteToDb: %w", err)
		}
	}

	return execRs, nil
}

// gets the pre-execute values for a block and sets the previous block hash
func (be *blockExecutor) getPreexecuteValues(blockNum uint64) (common.Hash, *types.Block, []common.Address, error) {
	preExecuteHeaderHash, err := rawdb.ReadCanonicalHash(be.tx, blockNum)
	if err != nil {
		return common.Hash{}, nil, nil, fmt.Errorf("ReadCanonicalHash: %w", err)
	}

	block, senders, err := be.cfg.blockReader.BlockWithSenders(be.ctx, be.tx, preExecuteHeaderHash, blockNum)
	if err != nil {
		return common.Hash{}, nil, nil, fmt.Errorf("BlockWithSenders: %w", err)
	}

	if block == nil {
		return common.Hash{}, nil, nil, fmt.Errorf("empty block blocknum: %d", blockNum)
	}

	block.HeaderNoCopy().ParentHash = be.prevBlockHash

	if be.cfg.chainConfig.IsLondon(blockNum) {
		parentHeader, err := be.cfg.blockReader.Header(be.ctx, be.tx, be.prevBlockHash, blockNum-1)
		if err != nil {
			return common.Hash{}, nil, nil, fmt.Errorf("cfg.blockReader.Header: %w", err)
		}
		block.HeaderNoCopy().BaseFee = misc.CalcBaseFeeZk(be.cfg.chainConfig, parentHeader)
	}

	return preExecuteHeaderHash, block, senders, nil
}

func (be *blockExecutor) postExecuteCommitValues(
	block *types.Block,
	senders []common.Address,
) error {
	header := block.Header()
	blockHash := header.Hash()
	blockNum := block.NumberU64()

	// if datastream hash was wrong, remove old data
	if blockHash != be.datastreamBlockHash {
		if be.cfg.chainConfig.IsForkId9Elderberry2(blockNum) {
			log.Warn(
				fmt.Sprintf("[%s] Blockhash mismatch", be.logPrefix),
				"blockNumber", blockNum,
				"datastreamBlockHash", be.datastreamBlockHash,
				"calculatedBlockHash", blockHash,
			)
		}
		if err := rawdbZk.DeleteSenders(be.tx, be.datastreamBlockHash, blockNum); err != nil {
			return fmt.Errorf("DeleteSenders: %w", err)
		}
		if err := rawdbZk.DeleteHeader(be.tx, be.datastreamBlockHash, blockNum); err != nil {
			return fmt.Errorf("DeleteHeader: %w", err)
		}

		bodyForStorage, err := rawdb.ReadBodyForStorageByKey(be.tx, dbutils.BlockBodyKey(blockNum, be.datastreamBlockHash))
		if err != nil {
			return fmt.Errorf("ReadBodyForStorageByKey: %w", err)
		}

		if err := rawdb.DeleteBodyAndTransactions(be.tx, blockNum, be.datastreamBlockHash); err != nil {
			return fmt.Errorf("DeleteBodyAndTransactions: %w", err)
		}
		if err := rawdb.WriteBodyAndTransactions(be.tx, blockHash, blockNum, block.Transactions(), bodyForStorage); err != nil {
			return fmt.Errorf("WriteBodyAndTransactions: %w", err)
		}

		// [zkevm] senders were saved in stage_senders for headerHashes based on incomplete headers
		// in stage execute we complete the headers and senders should be moved to the correct headerHash
		// also we should delete other data based on the old hash, since it is unaccessable now
		if err := rawdb.WriteSenders(be.tx, blockHash, blockNum, senders); err != nil {
			return fmt.Errorf("failed to write senders: %w", err)
		}
	}

	// TODO: how can we store this data right first time?  Or mop up old data as we're currently duping storage
	/*
			        ,     \    /      ,
			       / \    )\__/(     / \
			      /   \  (_\  /_)   /   \
			 ____/_____\__\@  @/___/_____\____
			|             |\../|              |
			|              \VV/               |
			|       ZKEVM duping storage      |
			|_________________________________|
			 |    /\ /      \\       \ /\    |
			 |  /   V        ))       V   \  |
			 |/     `       //        '     \|
			 `              V                '

		 we need to write the header back to the db at this point as the gas
		 used wasn't available from the data stream, or receipt hash, or bloom, so we're relying on execution to
		 provide it.  We also need to update the canonical hash, so we can retrieve this newly updated header
		 later.
	*/
	if err := rawdb.WriteHeader_zkEvm(be.tx, header); err != nil {
		return fmt.Errorf("WriteHeader_zkEvm: %w", err)
	}
	if err := rawdb.WriteHeadHeaderHash(be.tx, blockHash); err != nil {
		return fmt.Errorf("WriteHeadHeaderHash: %w", err)
	}
	if err := rawdb.WriteCanonicalHash(be.tx, blockHash, blockNum); err != nil {
		return fmt.Errorf("WriteCanonicalHash: %w", err)
	}

	// write the new block lookup entries
	if err := rawdb.WriteTxLookupEntries_zkEvm(be.tx, block); err != nil {
		return fmt.Errorf("WriteTxLookupEntries_zkEvm: %w", err)
	}

	return nil
}
