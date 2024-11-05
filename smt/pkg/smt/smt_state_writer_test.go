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
	type testCase struct {
		Witness   string                                `json:"witness"`
		StateRoot libcommon.Hash                        `json:"stateRoot"`
		Traces    map[libcommon.Address]*types.TxnTrace `json:"traces,omitempty"`
	}

	data, err := os.ReadFile("./testdata/smt-traces.json")
	require.NoError(t, err)

	var tc testCase
	err = json.Unmarshal(data, &tc)
	require.NoError(t, err)

	decodedWitnessRaw, err := hex.DecodeString(strings.TrimPrefix(tc.Witness, "0x"))
	require.NoError(t, err)

	witness, err := trie.NewWitnessFromReader(bytes.NewReader(decodedWitnessRaw), false)
	require.NoError(t, err)

	smt, err := BuildSMTfromWitness(witness)
	require.NoError(t, err)

	newSMT, err := smt.ApplyTraces(tc.Traces)
	require.NoError(t, err)

	require.Equal(t, tc.StateRoot, libcommon.BigToHash(newSMT.LastRoot()))
}
