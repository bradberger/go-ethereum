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
	"time"

	"github.com/davecheney/junk/clock"
)

const fcTimeConst = 1000000000

type ServerParams struct {
	BufLimit, MaxCost, MinRecharge uint64
}

type ClientNode struct {
	params   *ServerParams
	bufValue uint64
	lastTime int64
	lock     sync.Mutex
}

func NewClientNode(params *ServerParams) *ClientNode {
	return &ClientNode{
		params:   params,
		bufValue: params.BufLimit,
		lastTime: getTime(),
	}
}

func getTime() int64 {
	return clock.Monotonic.Now().UnixNano()
}

func (peer *ClientNode) recalcBV(time int64) {
	dt := uint64(time - peer.lastTime)
	if time < peer.lastTime {
		dt = 0
	}
	peer.bufValue += peer.params.MinRecharge * dt / fcTimeConst
	if peer.bufValue > peer.params.BufLimit {
		peer.bufValue = peer.params.BufLimit
	}
	peer.lastTime = time
}

func (peer *ClientNode) AcceptRequest() bool {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	peer.recalcBV(getTime())
	return peer.bufValue >= peer.params.MaxCost
}

func (peer *ClientNode) ChargeCost(cost uint64) (bv uint64) {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	if cost > peer.params.MaxCost {
		cost = peer.params.MaxCost
	}
	peer.recalcBV(getTime())
	peer.bufValue -= cost
	return peer.bufValue
}

type ServerNode struct {
	bufEstimate uint64
	lastTime    int64
	params      *ServerParams
	pending     map[uint64]uint64
	reqSent     uint64
	lock        sync.Mutex
}

func NewServerNode(params *ServerParams) *ServerNode {
	return &ServerNode{
		bufEstimate: params.BufLimit,
		lastTime:    getTime(),
		params:      params,
		pending:     make(map[uint64]uint64),
	}
}

func (peer *ServerNode) recalcBLE(time int64) {
	dt := uint64(time - peer.lastTime)
	if time < peer.lastTime {
		dt = 0
	}
	peer.bufEstimate += peer.params.MinRecharge * dt
	if peer.bufEstimate > peer.params.BufLimit {
		peer.bufEstimate = peer.params.BufLimit
	}
	peer.lastTime = time
}

func (peer *ServerNode) CanSend() time.Duration {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	if peer.bufEstimate >= peer.params.MaxCost {
		return 0
	}
	return time.Duration((peer.params.MaxCost - peer.bufEstimate) * fcTimeConst / peer.params.MinRecharge)
}

// blocks until request can be sent
func (peer *ServerNode) SendRequest(reqID uint64) {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	peer.recalcBLE(getTime())
	for peer.bufEstimate < peer.params.MaxCost {
		time.Sleep(peer.CanSend())
		peer.recalcBLE(getTime())
	}
	peer.bufEstimate -= peer.params.MaxCost
	peer.reqSent++
	if reqID >= 0 {
		peer.pending[reqID] = peer.reqSent
	}
}

func (peer *ServerNode) GotReply(reqID, bv uint64) {
	peer.lock.Lock()
	defer peer.lock.Unlock()

	rs, ok := peer.pending[reqID]
	if !ok {
		return
	}
	delete(peer.pending, reqID)
	peer.bufEstimate = bv - peer.params.MaxCost*(peer.reqSent-rs)
	peer.lastTime = getTime()
}
