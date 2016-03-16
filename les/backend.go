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

// Package les implements the Light Ethereum Subprotocol.
package les

import (
	"fmt"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	rpc "github.com/ethereum/go-ethereum/rpc"
)

type LightEthereum struct {
	*eth.Ethereum
	odr   light.OdrBackend
	relay light.TxRelayBackend
	// Handlers
	txPool          *light.TxPool
	blockchain      *light.LightChain
	protocolManager *ProtocolManager
}

func NewLightEthereum(ctx *node.ServiceContext, config *eth.Config) (*LightEthereum, error) {
	e, err := eth.New(ctx, config)
	if err != nil {
		return nil, err
	}
	chainDb := e.ChainDb()

	odr := NewLesOdr(chainDb)
	relay := NewLesTxRelay()
	eth := &LightEthereum{
		Ethereum: e,
		odr:      odr,
		relay:    relay,
	}

	//genesis := core.GenesisBlock(uint64(config.GenesisNonce), stateDb)
	eth.blockchain, err = light.NewLightChain(odr, eth.Pow(), eth.EventMux())
	if err != nil {
		if err == core.ErrNoGenesis {
			return nil, fmt.Errorf(`Genesis block not found. Please supply a genesis block with the "--genesis /path/to/file" argument`)
		}
		return nil, err
	}

	eth.txPool = light.NewTxPool(eth.EventMux(), eth.blockchain, eth.relay)
	if eth.protocolManager, err = NewProtocolManager(config.LightMode, config.NetworkId, eth.EventMux(), eth.Pow(), eth.blockchain, nil, chainDb, odr, relay); err != nil {
		return nil, err
	}

	return eth, nil
}

// APIs returns the collection of RPC services the ethereum package offers.
// NOTE, some of these services probably need to be moved to somewhere else.
// @TODO eth.NewPublicBlockChainAPI second argument needs a *core.Blockchain somehow from s.BlockChain()
// It might be that the API is removed because it just can't be done cleanly. 
func (s *LightEthereum) APIs() []rpc.API {
	apiBackend := &LesApiBackend{s}
	return []rpc.API{
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   eth.NewPublicEthereumAPI(apiBackend),
			Public:    true,
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   eth.NewPublicBlockChainAPI(apiBackend, nil, nil, s.ChainDb(), s.EventMux(), s.AccountManager()),
			Public:    true,
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   eth.NewPublicTransactionPoolAPI(apiBackend, s.ChainDb(), s.EventMux(), s.AccountManager()),
			Public:    true,
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   downloader.NewPublicDownloaderAPI(s.Downloader()),
			Public:    true,
		}, {
			Namespace: "txpool",
			Version:   "1.0",
			Service:   eth.NewPublicTxPoolAPI(apiBackend),
			Public:    true,
		}, {
			Namespace: "admin",
			Version:   "1.0",
			Service:   eth.NewPrivateAdminAPI(apiBackend),
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   eth.NewPublicDebugAPI(apiBackend),
			Public:    true,
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   eth.NewPrivateDebugAPI(apiBackend),
		},
	}
}

func (s *LightEthereum) ResetWithGenesisBlock(gb *types.Block) {
	s.blockchain.ResetWithGenesisBlock(gb)
}

func (s *LightEthereum) BlockChain() *light.LightChain      { return s.blockchain }
func (s *LightEthereum) TxPool() *light.TxPool              { return s.txPool }
func (s *LightEthereum) LesVersion() int                    { return int(s.protocolManager.SubProtocols[0].Version) }
func (s *LightEthereum) Downloader() *downloader.Downloader { return s.protocolManager.downloader }

// Protocols implements node.Service, returning all the currently configured
// network protocols to start.
func (s *LightEthereum) Protocols() []p2p.Protocol {
	return s.protocolManager.SubProtocols
}

// Start implements node.Service, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *LightEthereum) Start(*p2p.Server) error {
	s.protocolManager.Start()
	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *LightEthereum) Stop() error {
	s.blockchain.Stop()
	s.protocolManager.Stop()
	s.txPool.Stop()

	return s.Ethereum.Shutdown()
}
