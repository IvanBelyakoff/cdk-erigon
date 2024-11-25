package smt

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/status-im/keycard-go/hexutils"
)

type AddressData struct {
	Balance *big.Int
	Nonce   *big.Int
	Code    string
	Storage map[string]string
}

// BuildSMTfromWitness builds SMT from witness
func GetAddressValues(w *trie.Witness) (map[libcommon.Address]AddressData, error) {
	// using memdb
	s := NewSMT(nil, false)

	addressDataMap := make(map[libcommon.Address]AddressData)

	for i, operator := range w.Operators {
		switch op := operator.(type) {
		case *trie.OperatorSMTLeafValue:
			valScaler := big.NewInt(0).SetBytes(op.Value)
			addr := libcommon.BytesToAddress(op.Address)

			addressData, ok := addressDataMap[addr]
			if !ok {
				addressData = AddressData{}
			}

			switch op.NodeType {
			case utils.KEY_BALANCE:
				addressData.Balance = valScaler

			case utils.KEY_NONCE:
				addressData.Nonce = valScaler

			case utils.SC_STORAGE:
				if addressData.Storage == nil {
					addressData.Storage = make(map[string]string)
				}
				stKey := hexutils.BytesToHex(op.StorageKey)
				if len(stKey) > 0 {
					stKey = fmt.Sprintf("0x%s", stKey)
				}

				addressData.Storage[stKey] = valScaler.String()
			}

		case *trie.OperatorCode:
			addr := libcommon.BytesToAddress(w.Operators[i+1].(*trie.OperatorSMTLeafValue).Address)
			addressData, ok := addressDataMap[addr]
			if !ok {
				addressData = AddressData{}
			}
			code := hexutils.BytesToHex(op.Code)
			if len(code) > 0 {
				if err := s.Db.AddCode(hexutils.HexToBytes(code)); err != nil {
					return nil, err
				}
				code = fmt.Sprintf("0x%s", code)
			}

			addressData.Code = code

		case *trie.OperatorBranch:
		case *trie.OperatorHash:
		default:
			// Unsupported operator type
			return nil, fmt.Errorf("unsupported operator type: %T", op)
		}
	}

	return addressDataMap, nil
}
