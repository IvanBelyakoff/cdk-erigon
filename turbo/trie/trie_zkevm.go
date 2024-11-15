// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package trie implements Merkle Patricia Tries.
package trie

import (
	"fmt"
)

// Get returns the value for key stored in the trie.
func (t *Trie) GetAllAddresses() ([][]byte, error) {
	if t.root == nil {
		return nil, nil
	}

	accountAddresses := make([][]byte, 0, t.NumberOfAccounts())
	if err := getAllAddressesFromNode(t.root, &accountAddresses, []byte{}); err != nil {
		return nil, err
	}

	return accountAddresses, nil
}

func getAllAddressesFromNode(currentNode node, addresses *[][]byte, address []byte) error {
	switch n := (currentNode).(type) {
	case nil:
		return nil
	case *shortNode:
		key := append(address, n.Key...)
		switch n.Val.(type) {
		case *accountNode:
			*addresses = append(*addresses, hexToKeybytes(append(address, n.Key...)))
		default:
			return getAllAddressesFromNode(n.Val, addresses, key)
		}
	case *duoNode:
		i1, i2 := n.childrenIdx()
		tmpAddress := append(address, i1)
		if err := getAllAddressesFromNode(n.child1, addresses, tmpAddress); err != nil {
			return err
		}
		return getAllAddressesFromNode(n.child2, addresses, append(address, i2))
	case *fullNode:
		for index, child := range n.Children {
			if err := getAllAddressesFromNode(child, addresses, append(address, byte(index))); err != nil {
				return err
			}
		}
		return nil
	case hashNode:
		return nil
	case *accountNode:
		*addresses = append(*addresses, hexToKeybytes(address))
		return nil
	default:
		return fmt.Errorf("invalid node: %v", currentNode)
	}

	return nil
}
