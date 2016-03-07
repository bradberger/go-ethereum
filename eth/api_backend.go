// Copyright 2015 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/compiler"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/downloader"
	rpc "github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/net/context"
)

// EthApiBackend implements the ApiBackend for full nodes
type EthApiBackend struct {
	eth *FullEthereum
	gpo *GasPriceOracle
}

func (b *EthApiBackend) SetHead(number uint64) {
	b.eth.blockchain.SetHead(number)
}

func (b *EthApiBackend) HeaderByNumber(blockNr rpc.BlockNumber) *types.Header {
	// Pending block is only known by the miner
	if blockNr == rpc.PendingBlockNumber {
		return b.eth.miner.PendingBlock().Header()
	}
	// Otherwise resolve and return the block
	if blockNr == rpc.LatestBlockNumber {
		return b.eth.blockchain.CurrentHeader()
	}
	return b.eth.blockchain.GetHeaderByNumber(uint64(blockNr))
}

func (b *EthApiBackend) BlockByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.Block, error) {
	// Pending block is only known by the miner
	if blockNr == rpc.PendingBlockNumber {
		return b.eth.miner.PendingBlock(), nil
	}
	// Otherwise resolve and return the block
	if blockNr == rpc.LatestBlockNumber {
		return b.eth.blockchain.CurrentBlock(), nil
	}
	return b.eth.blockchain.GetBlockByNumber(uint64(blockNr)), nil
}

func (b *EthApiBackend) StateByNumber(blockNr rpc.BlockNumber) (ApiState, error) {
	// Pending state is only known by the miner
	if blockNr == rpc.PendingBlockNumber {
		return &EthApiState{b.eth.miner.PendingState()}, nil
	}
	// Otherwise resolve the block number and return its state
	header := b.HeaderByNumber(blockNr)
	if header == nil {
		return nil, nil
	}
	stateDb, err := state.New(header.Root, b.eth.chainDb)
	return &EthApiState{stateDb}, err
}

func (b *EthApiBackend) GetBlock(ctx context.Context, blockHash common.Hash) (*types.Block, error) {
	return b.eth.blockchain.GetBlock(blockHash), nil
}

func (b *EthApiBackend) GetTd(blockHash common.Hash) *big.Int {
	return b.eth.blockchain.GetTd(blockHash)
}

func (b *EthApiBackend) GetVMEnv(ctx context.Context, msg core.Message, header *types.Header) (vm.Environment, func() error, error) {
	stateDb, err := state.New(header.Root, b.eth.chainDb)
	if err != nil {
		return nil, nil, err
	}
	stateDb = stateDb.Copy()
	addr, _ := msg.From()
	from := stateDb.GetOrNewStateObject(addr)
	from.SetBalance(common.MaxBig)
	vmError := func() error { return nil }
	return core.NewEnv(stateDb, b.eth.blockchain, msg, header), vmError, nil
}

func (b *EthApiBackend) SendTx(ctx context.Context, signedTx *types.Transaction) error {
	b.eth.txPool.SetLocal(signedTx)
	return b.eth.txPool.Add(signedTx)
}

func (b *EthApiBackend) RemoveTx(txHash common.Hash) {
	b.eth.txPool.RemoveTx(txHash)
}

func (b *EthApiBackend) GetPoolTransactions() types.Transactions {
	return b.eth.txPool.GetTransactions()
}

func (b *EthApiBackend) GetPoolTransaction(txHash common.Hash) *types.Transaction {
	return b.eth.txPool.GetTransaction(txHash)
}

func (b *EthApiBackend) GetPoolNonce(ctx context.Context, addr common.Address) (uint64, error) {
	return b.eth.txPool.State().GetNonce(addr), nil
}

func (b *EthApiBackend) Stats() (pending int, queued int) {
	return b.eth.txPool.Stats()
}

func (b *EthApiBackend) TxPoolContent() (map[common.Address]map[uint64][]*types.Transaction, map[common.Address]map[uint64][]*types.Transaction) {
	return b.eth.TxPool().Content()
}

func (b *EthApiBackend) Solc() (*compiler.Solidity, error) {
	return b.eth.Solc()
}

func (b *EthApiBackend) SetSolc(solcPath string) (*compiler.Solidity, error) {
	return b.eth.SetSolc(solcPath)
}

func (b *EthApiBackend) Downloader() *downloader.Downloader {
	return b.eth.Downloader()
}

func (b *EthApiBackend) ProtocolVersion() int {
	return b.eth.EthVersion()
}

func (b *EthApiBackend) SuggestPrice() *big.Int {
	return b.gpo.SuggestPrice()
}

type EthApiState struct {
	state *state.StateDB
}

func (s *EthApiState) GetBalance(ctx context.Context, addr common.Address) (*big.Int, error) {
	return s.state.GetBalance(addr), nil
}

func (s *EthApiState) GetCode(ctx context.Context, addr common.Address) ([]byte, error) {
	return s.state.GetCode(addr), nil
}

func (s *EthApiState) GetState(ctx context.Context, a common.Address, b common.Hash) (common.Hash, error) {
	return s.state.GetState(a, b), nil
}

func (s *EthApiState) GetNonce(ctx context.Context, addr common.Address) (uint64, error) {
	return s.state.GetNonce(addr), nil
}