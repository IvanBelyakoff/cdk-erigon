package l1infotree_test

import (
	"context"
	"log"
	"math/big"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands/mocks"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/l1infotree"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// to be removed
func GetDbTx() (tx kv.RwTx, cleanup func()) {
	dbi, err := mdbx.NewTemporaryMdbx(context.Background(), "")
	if err != nil {
		panic(err)
	}
	tx, err = dbi.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}

	err = hermez_db.CreateHermezBuckets(tx)
	if err != nil {
		panic(err)
	}

	return tx, func() {
		tx.Rollback()
		dbi.Close()
	}
}

func TestNewHermezDb(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := hermez_db.NewHermezDb(tx)
	assert.NotNil(t, db)
}

func TestNewUpdater(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &ethconfig.Zk{}

	l1InfoTreeSyncer := syncer.NewL1Syncer(
		ctx,
		nil, nil, nil,
		cfg.L1BlockRange,       // cfg.L1BlockRange,
		cfg.L1QueryDelay,       // cfg.L1QueryDelay,
		cfg.L1HighestBlockType, // cfg.L1HighestBlockType,
	)

	updater := l1infotree.NewUpdater(cfg, l1InfoTreeSyncer)
	assert.NotNil(t, updater)
}

func TestUpdater_WarmUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	EthermanMock := mocks.NewMockIEtherman(mockCtrl)

	header := &types.Header{Number: big.NewInt(1), Difficulty: big.NewInt(100)}
	expectedBlock := types.NewBlockWithHeader(header)

	// Set up the mock expectation for BlockByNumber
	EthermanMock.EXPECT().
		BlockByNumber(gomock.Any(), (*big.Int)(nil)). // Matches any context and a nil blockNumber
		Return(expectedBlock, nil).
		AnyTimes()

	cfg := &ethconfig.Zk{}

	l1InfoTreeSyncer := syncer.NewL1Syncer(
		ctx,
		[]syncer.IEtherman{EthermanMock},
		[]common.Address{},
		[][]common.Hash{},
		cfg.L1BlockRange,       // cfg.L1BlockRange,
		cfg.L1QueryDelay,       // cfg.L1QueryDelay,
		cfg.L1HighestBlockType, // cfg.L1HighestBlockType,
	)

	updater := l1infotree.NewUpdater(cfg, l1InfoTreeSyncer)

	tx, cleanup := GetDbTx()
	defer cleanup()

	err := updater.WarmUp(tx)
	// log error
	log.Println(err)
	assert.NoError(t, err)
}

func TestUpdater_GetProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &ethconfig.Zk{}

	l1InfoTreeSyncer := syncer.NewL1Syncer(
		ctx,
		nil, nil, nil,
		cfg.L1BlockRange,       // cfg.L1BlockRange,
		cfg.L1QueryDelay,       // cfg.L1QueryDelay,
		cfg.L1HighestBlockType, // cfg.L1HighestBlockType,
	)

	updater := l1infotree.NewUpdater(cfg, l1InfoTreeSyncer)
	assert.NotNil(t, updater)

	progress := updater.GetProgress()
	assert.Equal(t, uint64(0), progress)
}

func TestUpdater_GetLatestUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &ethconfig.Zk{}

	l1InfoTreeSyncer := syncer.NewL1Syncer(
		ctx,
		nil, nil, nil,
		cfg.L1BlockRange,       // cfg.L1BlockRange,
		cfg.L1QueryDelay,       // cfg.L1QueryDelay,
		cfg.L1HighestBlockType, // cfg.L1HighestBlockType,
	)

	updater := l1infotree.NewUpdater(cfg, l1InfoTreeSyncer)
	assert.NotNil(t, updater)

	latestUpdate := updater.GetLatestUpdate()
	assert.Nil(t, latestUpdate)
}

func TestUpdater_CheckForInfoTreeUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	EthermanMock := mocks.NewMockIEtherman(mockCtrl)

	// header := &types.Header{Number: big.NewInt(1), Difficulty: big.NewInt(100)}
	// mockHeader := types.NewBlockWithHeader(header)

	// Define a header to be returned by our mock Ethereum calls
	mockHeader := &types.Header{
		Number:     big.NewInt(1),
		Time:       uint64(time.Now().Unix()),
		ParentHash: common.HexToHash("0x0"),
	}

	expectedBlock := types.NewBlockWithHeader(mockHeader)

	// Set up the mock expectation for BlockByNumber
	EthermanMock.EXPECT().
		BlockByNumber(gomock.Any(), (*big.Int)(nil)). // Matches any context and a nil blockNumber
		Return(expectedBlock, nil).
		AnyTimes()

	// Mock expectation: Whenever HeaderByNumber is called with any context and block number, return mockHeader
	EthermanMock.EXPECT().
		HeaderByNumber(gomock.Any(), gomock.Any()).
		Return(mockHeader, nil).
		AnyTimes() // Allow multiple calls to this method to always return our mock header

	// Minimal configuration for the syncer (can be empty if config values are unused in this test)
	cfg := &ethconfig.Zk{}

	l1InfoTreeSyncer := syncer.NewL1Syncer(
		ctx,
		[]syncer.IEtherman{EthermanMock},
		[]common.Address{},
		[][]common.Hash{},
		cfg.L1BlockRange,       // cfg.L1BlockRange,
		cfg.L1QueryDelay,       // cfg.L1QueryDelay,
		cfg.L1HighestBlockType, // cfg.L1HighestBlockType,
	)

	updater := l1infotree.NewUpdater(cfg, l1InfoTreeSyncer)

	tx, cleanup := GetDbTx()
	defer cleanup()

	err := updater.WarmUp(tx)
	assert.NoError(t, err)

	// func (u *Updater) CheckForInfoTreeUpdates(logPrefix string, tx kv.RwTx) (allLogs []types.Log, err error) {
	allLogs, err := updater.CheckForInfoTreeUpdates(
		"TestUpdater_CheckForInfoTreeUpdates",
		tx,
	)
	assert.NoError(t, err)
	assert.NotNil(t, allLogs)
}
