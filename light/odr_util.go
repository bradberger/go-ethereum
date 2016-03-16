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
package light

import (
	"bytes"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/net/context"
)

var ErrNoHeader = errors.New("Block header not found")

// retrieveContractCode tries to retrieve the contract code of the given account
// with the given hash from the network (id points to the storage trie belonging
// to the same account)
func retrieveContractCode(ctx context.Context, odr OdrBackend, id *TrieID, hash common.Hash) ([]byte, error) {
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

// GetBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func GetBodyRLP(ctx context.Context, odr OdrBackend, hash common.Hash) (rlp.RawValue, error) {
	if data := core.GetBodyRLP(odr.Database(), hash); data != nil {
		return data, nil
	}
	r := &BlockRequest{Hash: hash}
	if err := odr.Retrieve(ctx, r); err != nil {
		return nil, err
	} else {
		return r.Rlp, nil
	}
}

// GetBody retrieves the block body (transactons, uncles) corresponding to the
// hash.
func GetBody(ctx context.Context, odr OdrBackend, hash common.Hash) (*types.Body, error) {
	data, err := GetBodyRLP(ctx, odr, hash)
	if err != nil {
		return nil, err
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		glog.V(logger.Error).Infof("invalid block body RLP for hash %x: %v", hash, err)
		return nil, err
	}
	return body, nil
}

// GetBlock retrieves an entire block corresponding to the hash, assembling it
// back from the stored header and body.
func GetBlock(ctx context.Context, odr OdrBackend, hash common.Hash) (*types.Block, error) {
	// Retrieve the block header and body contents
	header := core.GetHeader(odr.Database(), hash)
	if header == nil {
		return nil, ErrNoHeader
	}
	body, err := GetBody(ctx, odr, hash)
	if err != nil {
		return nil, err
	}
	// Reassemble the block and return
	return types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles), nil
}

// GetBlockReceipts retrieves the receipts generated by the transactions included
// in a block given by its hash.
func GetBlockReceipts(ctx context.Context, odr OdrBackend, hash common.Hash) (types.Receipts, error) {
	receipts := core.GetBlockReceipts(odr.Database(), hash)
	if receipts != nil {
		return receipts, nil
	}
	r := &ReceiptsRequest{Hash: hash}
	if err := odr.Retrieve(ctx, r); err != nil {
		return nil, err
	} else {
		return r.Receipts, nil
	}
}
