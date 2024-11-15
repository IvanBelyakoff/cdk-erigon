// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"
	"math/big"
	"math/rand"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/stretchr/testify/assert"
)

func TestGetAllAccounts(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	numberOfAccounts := 2000

	addresses := make([][]byte, numberOfAccounts)
	for i := 0; i < len(addresses); i++ {
		a := getAddressForIndex(i)
		addresses[i] = crypto.Keccak256(a[:])
	}
	codeValues := make([][]byte, len(addresses))
	for i := 0; i < len(addresses); i++ {
		codeValues[i] = genRandomByteArrayOfLen(128)
		codeHash := libcommon.BytesToHash(crypto.Keccak256(codeValues[i]))
		balance := new(big.Int).Rand(random, new(big.Int).Exp(libcommon.Big2, libcommon.Big256, nil))
		acc := accounts.NewAccount()
		acc.Nonce = uint64(random.Int63())
		acc.Balance.SetFromBig(balance)
		acc.Root = EmptyRoot
		acc.CodeHash = codeHash

		trie.UpdateAccount(addresses[i][:], &acc)
		err := trie.UpdateAccountCode(addresses[i][:], codeValues[i])
		assert.Nil(t, err, "should successfully insert code")
	}

	addressesFound, err := trie.GetAllAddresses()
	assert.Nil(t, err, "should successfully get all accounts")
	assert.Equal(t, len(addresses), len(addressesFound), "should receive the right number of accounts")
	for i := 0; i < len(addresses); i++ {
		found := false
		for j := 0; j < len(addressesFound); j++ {
			if bytes.Compare(addresses[i], addressesFound[j]) == 0 {
				found = true
				break
			}
		}

		assert.True(t, found, "address not found")
	}
}
