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

// Package flowcontrol implements a client side flow control mechanism
package flowcontrol

import (
	"sync"
)

type ClientManager struct {
	params *ServerParams
	peers  map[string]*ClientNode
	lock   sync.Mutex
}

func NewClientManager(params *ServerParams) *ClientManager {
	return &ClientManager{
		params: params,
		peers:  make(map[string]*ClientNode),
	}
}

func (self *ClientManager) AddClient(peerID string) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.peers[peerID] = NewClientNode(self.params)
}

func (self *ClientManager) RemoveClient(peerID string) {
	self.lock.Lock()
	defer self.lock.Unlock()

	delete(self.peers, peerID)
}

func (self *ClientManager) AcceptRequest(peerID string) bool {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.peers[peerID].AcceptRequest()
}

func (self *ClientManager) ChargeCost(peerID string, cost uint64) (bv uint64) {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.peers[peerID].ChargeCost(cost)
}

func (self *ClientManager) RequestProcessed(peerID string) {
	self.lock.Lock()
	defer self.lock.Unlock()

}
