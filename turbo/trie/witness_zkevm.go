package trie

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/status-im/keycard-go/hexutils"
)

type AddressData struct {
	Balance *big.Int
	Nonce   *big.Int
	Code    []byte
	Storage map[string]string
}

// BuildSMTfromWitness builds SMT from witness
func (w *Witness) GetAddressValues() (map[libcommon.Address]AddressData, error) {
	addressDataMap := make(map[libcommon.Address]AddressData)

	for i, operator := range w.Operators {
		switch op := operator.(type) {
		case *OperatorSMTLeafValue:
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
			addressDataMap[addr] = addressData
		case *OperatorCode:
			addr := libcommon.BytesToAddress(w.Operators[i+1].(*OperatorSMTLeafValue).Address)
			addressData, ok := addressDataMap[addr]
			if !ok {
				addressData = AddressData{}
			}
			addressData.Code = op.Code
			addressDataMap[addr] = addressData
		case *OperatorBranch:
		case *OperatorHash:
		default:
			// Unsupported operator type
			return nil, fmt.Errorf("unsupported operator type: %T", op)
		}
	}

	return addressDataMap, nil
}
