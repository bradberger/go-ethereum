// Copyright 2015 The go-ethereum Authors
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

// Package light implements on-demand retrieval capable state and chain objects
// for the Ethereum Light Client.
package light

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/net/context"
)

// NoOdr is the default context passed to an ODR capable function when the ODR
// service is not required.
var NoOdr = context.Background()

// OdrBackend is an interface to a backend service that handles ODR retrievals
type OdrBackend interface {
	Database() ethdb.Database
	Retrieve(ctx context.Context, req OdrRequest) error
}

// OdrRequest is an interface for retrieval requests
type OdrRequest interface {
	StoreResult(db ethdb.Database)
}

// TrieID identifies a state or account storage trie
type TrieID struct {
	BlockHash, Root common.Hash
	AccKey          []byte
}

// StateTrieID returns a TrieID for a state trie belonging to a certain block
// header.
func StateTrieID(header *types.Header) *TrieID {
	return &TrieID{
		BlockHash: header.Hash(),
		AccKey:    nil,
		Root:      header.Root,
	}
}

// StorageTrieID returns a TrieID for a contract storage trie at a given account
// of a given state trie. It also requires the root hash of the trie for
// checking Merkle proofs.
func StorageTrieID(state *TrieID, addr common.Address, root common.Hash) *TrieID {
	return &TrieID{
		BlockHash: state.BlockHash,
		AccKey:    crypto.Keccak256(addr[:]),
		Root:      root,
	}
}

// TrieRequest is the ODR request type for state/storage trie entries
type TrieRequest struct {
	OdrRequest
	Root  common.Hash
	Key   []byte
	Proof []rlp.RawValue
}

// StoreResult stores the retrieved data in local database
func (req *TrieRequest) StoreResult(db ethdb.Database) {
	storeProof(db, req.Proof)
}

// storeProof stores the new trie nodes obtained from a merkle proof in the database
func storeProof(db ethdb.Database, proof []rlp.RawValue) {
	for _, buf := range proof {
		hash := crypto.Keccak256(buf)
		val, _ := db.Get(hash)
		if val == nil {
			db.Put(hash, buf)
		}
	}
}

type CodeRequest struct {
	OdrRequest
	Id    *TrieID
	Hash common.Hash
	Data []byte
}

func (req *CodeRequest) StoreResult(db ethdb.Database) {
	db.Put(req.Hash[:], req.Data)
}

// NodeDataRequest is the ODR request type for node data (used for retrieving contract code)
type NodeDataRequest struct {
	OdrRequest
	Hash common.Hash
	Data []byte
}

// GetData returns the retrieved node data after a successful request
func (req *NodeDataRequest) GetData() []byte {
	return req.Data
}

// StoreResult stores the retrieved data in local database
func (req *NodeDataRequest) StoreResult(db ethdb.Database) {
	db.Put(req.Hash[:], req.GetData())
}

type BlockRequest struct {
	OdrRequest
	Hash common.Hash
	Rlp  []byte
}

func (req *BlockRequest) StoreResult(db ethdb.Database) {
	core.WriteBodyRLP(db, req.Hash, req.Rlp)
}

type ReceiptsRequest struct {
	OdrRequest
	Hash     common.Hash
	Receipts types.Receipts
}

func (req *ReceiptsRequest) StoreResult(db ethdb.Database) {
	core.WriteBlockReceipts(db, req.Hash, req.Receipts)
}

var sha3_nil = crypto.Keccak256Hash(nil)

// retrieveNodeData tries to retrieve node data with the given hash from the network
func retrieveNodeData(ctx context.Context, odr OdrBackend, hash common.Hash) ([]byte, error) {
	if hash == sha3_nil {
		return nil, nil
	}
	res, _ := odr.Database().Get(hash[:])
	if res != nil {
		return res, nil
	}
	r := &NodeDataRequest{Hash: hash}
	if err := odr.Retrieve(ctx, r); err != nil {
		return nil, err
	} else {
		return r.GetData(), nil
	}
}
