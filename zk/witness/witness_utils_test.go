package witness

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"math/rand"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/stretchr/testify/assert"
)

func TestMergeWitnesses(t *testing.T) {
	trie1 := trie.New(common.Hash{})
	trie2 := trie.New(common.Hash{})
	trieFull := trie.New(common.Hash{})

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
		codeHash := common.BytesToHash(crypto.Keccak256(codeValues[i]))
		balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		acc := accounts.NewAccount()
		acc.Nonce = uint64(random.Int63())
		acc.Balance.SetFromBig(balance)
		acc.Root = trie.EmptyRoot
		acc.CodeHash = codeHash

		if i&1 == 0 {
			trie1.UpdateAccount(addresses[i][:], &acc)
			err := trie1.UpdateAccountCode(addresses[i][:], codeValues[i])
			assert.Nil(t, err, "should successfully insert code")
		} else {
			trie2.UpdateAccount(addresses[i][:], &acc)
			err := trie2.UpdateAccountCode(addresses[i][:], codeValues[i])
			assert.Nil(t, err, "should successfully insert code")
		}
		trieFull.UpdateAccount(addresses[i][:], &acc)
		err := trieFull.UpdateAccountCode(addresses[i][:], codeValues[i])
		assert.Nil(t, err, "should successfully insert code")
	}

	witness1, err := trie1.ExtractWitness(false, nil)
	assert.Nil(t, err, "should successfully extract witness")
	witness2, err := trie2.ExtractWitness(false, nil)
	assert.Nil(t, err, "should successfully extract witness")
	witnessFull, err := trieFull.ExtractWitness(false, nil)
	assert.Nil(t, err, "should successfully extract witness")

	mergedWitness, err := MergeWitnesses([]*trie.Witness{witness1, witness2})
	assert.Nil(t, err, "should successfully merge witnesses")

	//create writer
	var buff bytes.Buffer
	mergedWitness.WriteDiff(witnessFull, &buff)
	diff := buff.String()
	assert.Equal(t, 0, len(diff), "witnesses should be equal")
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
