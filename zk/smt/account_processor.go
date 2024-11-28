package smt

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/status-im/keycard-go/hexutils"
)

//go:generate mockgen -typed=true -destination=./mocks/account_collector_db_mock.go -package=mocks . AccountCollectorDb

type AccountCollectorDb interface {
	InsertKeySource(key utils.NodeKey, value []byte) error
	InsertAccountValue(key utils.NodeKey, value utils.NodeValue8) error
}

type accountCollector struct {
	db   AccountCollectorDb
	psr  *state.PlainStateReader
	keys []utils.NodeKey
}

func NewAccountCollector(db AccountCollectorDb, psr *state.PlainStateReader) *accountCollector {
	return &accountCollector{
		db:   db,
		psr:  psr,
		keys: []utils.NodeKey{},
	}
}

// parses the data and saves it to the DB
// collects the saved keys in the array "keys"
func (ac *accountCollector) processAccount(a *accounts.Account, as map[string]string, inc uint64, addr common.Address) error {
	// get the account balance and nonce
	if err := ac.insertAccountStateToKV(addr.String(), a.Balance.ToBig(), new(big.Int).SetUint64(a.Nonce)); err != nil {
		return fmt.Errorf("insertAccountStateToKV: %w", err)
	}

	// store the contract bytecode
	cc, err := ac.psr.ReadAccountCode(addr, inc, a.CodeHash)
	if err != nil {
		return fmt.Errorf("ReadAccountCode: %w", err)
	}

	if len(cc) > 0 {
		ach := hexutils.BytesToHex(cc)
		hexcc := "0x" + ach
		if err = ac.insertContractBytecodeToKV(addr.String(), hexcc); err != nil {
			return fmt.Errorf("insertContractBytecodeToKV: %w", err)
		}
	}

	if len(as) > 0 {
		// store the account storage
		if err = ac.insertContractStorageToKV(addr.String(), as); err != nil {
			return fmt.Errorf("insertContractStorageToKV: %w", err)
		}
	}

	return nil
}

func (ac *accountCollector) insertContractBytecodeToKV(ethAddr string, bytecode string) error {
	keyContractCode := utils.KeyContractCode(ethAddr)
	keyContractLength := utils.KeyContractLength(ethAddr)
	bi := utils.HashContractBytecodeBigInt(bytecode)

	parsedBytecode := strings.TrimPrefix(bytecode, "0x")
	if len(parsedBytecode)%2 != 0 {
		parsedBytecode = "0" + parsedBytecode
	}

	bytecodeLength := len(parsedBytecode) / 2

	if err := ac.parseAndInsertKV(utils.SC_CODE, ethAddr, keyContractCode, bi, common.Hash{}); err != nil {
		return fmt.Errorf("parseAndInsertKV: %w", err)
	}

	if err := ac.parseAndInsertKV(utils.SC_LENGTH, ethAddr, keyContractLength, big.NewInt(int64(bytecodeLength)), common.Hash{}); err != nil {
		return fmt.Errorf("parseAndInsertKV: %w", err)
	}

	return nil
}

func (ac *accountCollector) insertContractStorageToKV(ethAddr string, storage map[string]string) (err error) {
	add := utils.ScalarToArrayBig(utils.ConvertHexToBigInt(ethAddr))

	var keyStoragePosition utils.NodeKey
	for k, v := range storage {
		if v == "" {
			continue
		}

		keyStoragePosition = utils.KeyContractStorage(add, k)

		base := 10
		if strings.HasPrefix(v, "0x") {
			v = v[2:]
			base = 16
		}

		val, _ := new(big.Int).SetString(v, base)
		sp, _ := utils.StrValToBigInt(k)
		if err := ac.parseAndInsertKV(utils.SC_STORAGE, ethAddr, keyStoragePosition, val, common.BigToHash(sp)); err != nil {
			return fmt.Errorf("parseAndInsertKV: %w", err)
		}
	}

	return nil
}

func (ac *accountCollector) insertAccountStateToKV(ethAddr string, balance, nonce *big.Int) error {
	keyBalance := utils.KeyEthAddrBalance(ethAddr)
	keyNonce := utils.KeyEthAddrNonce(ethAddr)

	if err := ac.parseAndInsertKV(utils.KEY_BALANCE, ethAddr, keyBalance, balance, common.Hash{}); err != nil {
		return fmt.Errorf("parseAndInsertKV: %w", err)
	}

	if err := ac.parseAndInsertKV(utils.KEY_NONCE, ethAddr, keyNonce, nonce, common.Hash{}); err != nil {
		return fmt.Errorf("parseAndInsertKV: %w", err)
	}
	return nil
}

func (ac *accountCollector) parseAndInsertKV(encodeKey int, ethAddr string, key utils.NodeKey, val *big.Int, storagePos common.Hash) error {
	x := utils.ScalarToArrayBig(val)
	valueNode, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return fmt.Errorf("NodeValue8FromBigIntArray: %w", err)
	}

	if !valueNode.IsZero() {
		if err := ac.insertKVtoDb(encodeKey, ethAddr, key, valueNode, storagePos); err != nil {
			return fmt.Errorf("processAccount: %w", err)
		}
	}

	return nil
}

func (ac *accountCollector) insertKVtoDb(encodeKey int, ethAddr string, key utils.NodeKey, val *utils.NodeValue8, storagePos common.Hash) error {
	if err := ac.db.InsertAccountValue(key, *val); err != nil {
		return fmt.Errorf("InsertAccountValue: %w", err)
	}

	ks := utils.EncodeKeySource(encodeKey, utils.ConvertHexToAddress(ethAddr), storagePos)
	if err := ac.db.InsertKeySource(key, ks); err != nil {
		return fmt.Errorf("InsertKeySource: %w", err)
	}

	ac.keys = append(ac.keys, key)

	return nil
}
