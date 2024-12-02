package smt

import (
	"context"
	"errors"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/zk/smt/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

type insertKeySourceParams struct {
	key    utils.NodeKey
	value  []byte
	result error
}

type insertAccountValueParams struct {
	key    utils.NodeKey
	value  utils.NodeValue8
	result error
}
type testStruct struct {
	name                     string
	encodeKey                int
	ethAddress               string
	key                      utils.NodeKey
	val                      utils.NodeValue8
	insertKeySourceParams    *insertKeySourceParams
	insertAccountValueParams *insertAccountValueParams
	resultError              string
}

func Test_insertKVtoDb(t *testing.T) {
	tests := []testStruct{
		{
			name:                     "inserted key in keys array",
			encodeKey:                1,
			ethAddress:               "0x1234567890abcdef",
			key:                      utils.NodeKey{1, 2, 3, 4},
			val:                      utils.NodeValue8{},
			insertKeySourceParams:    nil,
			insertAccountValueParams: nil,
			resultError:              "",
		},
		{
			name:       "InsertAccountValue inserts correct values",
			encodeKey:  1,
			ethAddress: "0x1234567890abcdef",
			key:        utils.NodeKey{1, 2, 3, 4},
			val:        utils.NodeValue8{},
			insertAccountValueParams: &insertAccountValueParams{
				key:    utils.NodeKey{1, 2, 3, 4},
				value:  utils.NodeValue8{},
				result: nil,
			},
			insertKeySourceParams: nil,
			resultError:           "",
		},
		{
			name:       "InsertAccountValue returns err",
			encodeKey:  1,
			ethAddress: "0x1234567890abcdef",
			key:        utils.NodeKey{1, 2, 3, 4},
			val:        utils.NodeValue8{},
			insertAccountValueParams: &insertAccountValueParams{
				key:    utils.NodeKey{1, 2, 3, 4},
				value:  utils.NodeValue8{},
				result: errors.New("error"),
			},
			insertKeySourceParams: nil,
			resultError:           "InsertAccountValue",
		},
		{
			name:                     "InsertKeySource inserts correct values",
			encodeKey:                1,
			ethAddress:               "0x1234567890abcdef",
			key:                      utils.NodeKey{1, 2, 3, 4},
			val:                      utils.NodeValue8{},
			insertAccountValueParams: nil,
			insertKeySourceParams: &insertKeySourceParams{
				key:    utils.NodeKey{1, 2, 3, 4},
				value:  []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 52, 86, 120, 144, 171, 205, 239},
				result: nil,
			},
			resultError: "",
		},
		{
			name:                     "InsertKeySource returns err",
			encodeKey:                1,
			ethAddress:               "0x1234567890abcdef",
			key:                      utils.NodeKey{1, 2, 3, 4},
			val:                      utils.NodeValue8{},
			insertAccountValueParams: nil,
			insertKeySourceParams: &insertKeySourceParams{
				key:    utils.NodeKey{1, 2, 3, 4},
				value:  []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 52, 86, 120, 144, 171, 205, 239},
				result: errors.New("error"),
			},
			resultError: "InsertKeySource",
		},
	}
	db := memdb.NewTestDB(t)
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	assert.NoError(t, err)
	defer tx.Rollback()
	ctrl := gomock.NewController(t)
	psr := state.NewPlainStateReader(tx)
	defer ctrl.Finish()

	for _, tc := range tests {
		eriDb := mocks.NewMockAccountCollectorDb(ctrl)
		if tc.insertKeySourceParams != nil {
			eriDb.EXPECT().InsertKeySource(tc.insertKeySourceParams.key, tc.insertKeySourceParams.value).Return(tc.insertKeySourceParams.result).Times(1)
		} else {
			eriDb.EXPECT().InsertKeySource(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		}
		if tc.insertAccountValueParams != nil {
			eriDb.EXPECT().InsertAccountValue(tc.insertAccountValueParams.key, tc.insertAccountValueParams.value).Return(tc.insertAccountValueParams.result).Times(1)
		} else {
			eriDb.EXPECT().InsertAccountValue(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		}

		accCollector := NewAccountCollector(eriDb, psr)
		err = accCollector.insertKVtoDb(tc.encodeKey, tc.ethAddress, tc.key, &tc.val, common.Hash{})
		if tc.resultError != "" {
			assert.ErrorContains(t, err, tc.resultError)
			assert.Equal(t, 0, len(accCollector.keys))
		} else {
			assert.NoError(t, err)
			assert.Equal(t, 1, len(accCollector.keys))
			assert.Equal(t, tc.key, accCollector.keys[0])
		}
	}
}
