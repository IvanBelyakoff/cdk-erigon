package smt

import (
	"encoding/hex"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
)

// ApplyTraces applies a map of traces on the given SMT and returns new instance of SMT, without altering the original one.
func (s *SMT) ApplyTraces(traces map[libcommon.Address]types.TxnTrace) (*SMT, error) {
	result := NewSMT(s.Db, false)

	for addr, trace := range traces {
		if trace.SelfDestructed != nil && *trace.SelfDestructed {
			nodeVal, err := s.getValue(utils.KEY_NONCE, addr, nil)
			if err != nil {
				return nil, err
			}

			if len(nodeVal) > 0 {
				return nil, fmt.Errorf("account %s is annotated to be self-destructed, but it already exists in the SMT", addr)
			}

			continue
		}

		addrString := addr.Hex()
		// Set account balance
		if trace.Balance != nil {
			if _, err := result.SetAccountBalance(addrString, trace.Balance.ToBig()); err != nil {
				return nil, err
			}
		}

		// Set account nonce
		if trace.Nonce != nil {
			if _, err := result.SetAccountNonce(addrString, trace.Nonce.ToBig()); err != nil {
				return nil, err
			}
		}

		// Set account storage map
		storageMap := make(map[string]string)
		for h, storageSlot := range trace.StorageWritten {
			storageMap[hex.EncodeToString(h[:])] = storageSlot.Hex()
		}

		if _, err := result.SetContractStorage(addrString, storageMap, nil); err != nil {
			return nil, err
		}

		// Set account bytecode
		if trace.CodeUsage != nil {
			if err := result.SetContractBytecode(addrString, hex.EncodeToString(trace.CodeUsage.Write)); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}
