package witness

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/status-im/keycard-go/hexutils"
	"github.com/stretchr/testify/assert"
)

func TestMergeWitnesses(t *testing.T) {
	smt1 := smt.NewSMT(nil, false)
	smt2 := smt.NewSMT(nil, false)
	smtFull := smt.NewSMT(nil, false)

	random := rand.New(rand.NewSource(0))

	numberOfAccounts := 500
	addresses := make(map[string]trie.AddressData, numberOfAccounts)

	for i := 0; i < numberOfAccounts; i++ {
		a := getAddressForIndex(i)
		addressBytes := crypto.Keccak256(a[:])
		address := common.BytesToAddress(addressBytes).String()
		balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		nonce := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		bytecode := "afafaf"
		contractStorage := make(map[string]string)
		for j := 0; j < 10; j++ {
			storageKey := genRandomByteArrayOfLen(32)
			storageValue := genRandomByteArrayOfLen(32)
			contractStorage[common.BytesToHash(storageKey).Hex()] = common.BytesToHash(storageValue).Hex()
		}

		addresses[address] = trie.AddressData{
			Balance: balance,
			Nonce:   nonce,
		}
		var smtPart *smt.SMT

		if i&1 == 0 {
			smtPart = smt1
		} else {
			smtPart = smt2
		}

		if _, err := smtPart.SetAccountBalance(address, balance); err != nil {
			t.Error(err)
			return
		}
		if _, err := smtPart.SetAccountNonce(address, nonce); err != nil {
			t.Error(err)
			return
		}
		if err := smtPart.SetContractBytecode(address, bytecode); err != nil {
			t.Error(err)
			return
		}
		if err := smtPart.Db.AddCode(hexutils.HexToBytes(bytecode)); err != nil {
			t.Error(err)
			return
		}
		if _, err := smtPart.SetContractStorage(address, contractStorage, nil); err != nil {
			t.Error(err)
			return
		}

		if _, err := smtFull.SetAccountBalance(address, balance); err != nil {
			t.Error(err)
			return
		}
		if _, err := smtFull.SetAccountNonce(address, nonce); err != nil {
			t.Error(err)
			return
		}
		if err := smtFull.SetContractBytecode(address, bytecode); err != nil {
			t.Error(err)
			return
		}
		if err := smtFull.Db.AddCode(hexutils.HexToBytes(bytecode)); err != nil {
			t.Error(err)
			return
		}
		if _, err := smtFull.SetContractStorage(address, contractStorage, nil); err != nil {
			t.Error(err)
			return
		}
	}

	rl1 := &trie.AlwaysTrueRetainDecider{}
	rl2 := &trie.AlwaysTrueRetainDecider{}
	rlFull := &trie.AlwaysTrueRetainDecider{}
	witness1, err := smt1.BuildWitness(rl1, context.Background())
	if err != nil {
		t.Error(err)
		return
	}

	witness2, err := smt2.BuildWitness(rl2, context.Background())
	if err != nil {
		t.Error(err)
		return
	}

	witnessFull, err := smtFull.BuildWitness(rlFull, context.Background())
	if err != nil {
		t.Error(err)
		return
	}
	mergedWitness, err := MergeWitnesses(context.Background(), []*trie.Witness{witness1, witness2})
	assert.Nil(t, err, "should successfully merge witnesses")

	//create writer
	var buff bytes.Buffer
	mergedWitness.WriteDiff(witnessFull, &buff)
	diff := buff.String()
	assert.Equal(t, 0, len(diff), "witnesses should be equal")
	if len(diff) > 0 {
		fmt.Println(diff)
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

func TestMergeRealWitnesses(t *testing.T) {
	blockWitness1, err := ParseWitnessFromBytes(witness1, false)
	if err != nil {
		t.Error(err)
		return
	}

	blockWitness2, err := ParseWitnessFromBytes(witness2, false)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = MergeWitnesses(context.Background(), []*trie.Witness{blockWitness1, blockWitness2})
	if err != nil {
		t.Error(err)
		return
	}
}
