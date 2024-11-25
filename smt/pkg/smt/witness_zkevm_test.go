package smt

import (
	"context"
	"encoding/binary"
	"math/big"
	"math/rand"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

func TestGetAddressValues(t *testing.T) {
	numberOfAccounts := 1000
	random := rand.New(rand.NewSource(0))
	smt1 := NewSMT(nil, false)

	addresses := make(map[string]AddressData, numberOfAccounts)
	for i := 0; i < len(addresses); i++ {
		a := getAddressForIndex(i)
		addressBytes := crypto.Keccak256(a[:])
		address := common.BytesToAddress(addressBytes).String()
		balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		nonce := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

		addresses[address] = AddressData{
			Balance: balance,
			Nonce:   nonce,
		}

		if _, err := smt1.SetAccountBalance(address, balance); err != nil {
			t.Error(err)
			return
		}
		if _, err := smt1.SetAccountNonce(address, nonce); err != nil {
			t.Error(err)
			return
		}
	}

	// witness rom smt
	rl := &trie.AlwaysTrueRetainDecider{}

	witness1, err := BuildWitness(smt1.RoSMT, rl, context.Background())
	if err != nil {
		t.Error(err)
		return
	}

	addressDataMap, err := GetAddressValues(witness1)
	if err != nil {
		t.Error(err)
		return
	}

	if len(addressDataMap) != len(addresses) {
		t.Errorf("expected %d addresses, got %d", len(addresses), len(addressDataMap))
		return
	}

	for address, data := range addressDataMap {
		expectedData, ok := addresses[address.Hex()]
		if !ok {
			t.Errorf("address %s not found", address)
			return
		}

		if expectedData.Balance.Cmp(data.Balance) != 0 {
			t.Errorf("expected balance %s, got %s", expectedData.Balance, data.Balance)
			return
		}

		if expectedData.Nonce.Cmp(data.Nonce) != 0 {
			t.Errorf("expected nonce %s, got %s", expectedData.Nonce, data.Nonce)
			return
		}
	}

}

func getAddressForIndex(index int) [20]byte {
	var address [20]byte
	binary.BigEndian.PutUint32(address[:], uint32(index))
	return address
}

func genRandomByteArrayOfLen(length uint) []byte {
	array := make([]byte, length)
	for i := uint(0); i < length; i++ {
		array[i] = byte(rand.Intn(256))
	}
	return array
}
