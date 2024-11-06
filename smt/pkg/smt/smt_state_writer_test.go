package smt

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/stretchr/testify/require"
)

func TestSMTApplyTraces(t *testing.T) {
	type testData struct {
		Witness   string                                `json:"witness"`
		StateRoot libcommon.Hash                        `json:"stateRoot"`
		Traces    map[libcommon.Address]*types.TxnTrace `json:"traces,omitempty"`
	}

	cases := []struct {
		name string
		file string
	}{
		{
			name: "Empty block traces",
			file: "./testdata/zerotraces/empty-block.json",
		},
		{
			name: "Non-empty block traces",
			file: "./testdata/zerotraces/non-empty-block.json",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rawTestData, err := os.ReadFile(c.file)
			require.NoError(t, err)

			var td testData
			err = json.Unmarshal(rawTestData, &td)
			require.NoError(t, err)

			decodedWitnessRaw, err := hex.DecodeString(strings.TrimPrefix(td.Witness, "0x"))
			require.NoError(t, err)

			witness, err := trie.NewWitnessFromReader(bytes.NewReader(decodedWitnessRaw), false)
			require.NoError(t, err)

			smt, err := BuildSMTfromWitness(witness)
			require.NoError(t, err)

			newSMT, err := smt.ApplyTraces(td.Traces)
			require.NoError(t, err)

			require.Equal(t, td.StateRoot, libcommon.BigToHash(newSMT.LastRoot()))
		})
	}
}
