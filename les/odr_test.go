package les

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/light"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"golang.org/x/net/context"
)

type odrTestFn func(ctx context.Context, db ethdb.Database, bc *core.BlockChain, lc *light.LightChain, bhash common.Hash) []byte

func TestOdrGetBlockLes1(t *testing.T) { testOdr(t, 1, 1, odrGetBlock) }

func odrGetBlock(ctx context.Context, db ethdb.Database, bc *core.BlockChain, lc *light.LightChain, bhash common.Hash) []byte {
	var block *types.Block
	if bc != nil {
		block = bc.GetBlock(bhash)
	} else {
		block, _ = lc.GetBlock(ctx, bhash)
	}
	if block == nil {
		return nil
	}
	rlp, _ := rlp.EncodeToBytes(block)
	return rlp
}

func TestOdrGetReceiptsLes1(t *testing.T) { testOdr(t, 1, 1, odrGetReceipts) }

func odrGetReceipts(ctx context.Context, db ethdb.Database, bc *core.BlockChain, lc *light.LightChain, bhash common.Hash) []byte {
	var receipts types.Receipts
	if bc != nil {
		receipts = core.GetBlockReceipts(db, bhash)
	} else {
		receipts, _ = light.GetBlockReceipts(ctx, lc.Odr(), bhash)
	}
	if receipts == nil {
		return nil
	}
	rlp, _ := rlp.EncodeToBytes(receipts)
	return rlp
}

func TestOdrAccountsLes1(t *testing.T) { testOdr(t, 1, 1, odrAccounts) }

func odrAccounts(ctx context.Context, db ethdb.Database, bc *core.BlockChain, lc *light.LightChain, bhash common.Hash) []byte {
	dummyAddr := common.HexToAddress("1234567812345678123456781234567812345678")
	acc := []common.Address{testBankAddress, acc1Addr, acc2Addr, dummyAddr}

	trie.ClearGlobalCache()

	var res []byte
	for _, addr := range acc {
		if bc != nil {
			header := bc.GetHeader(bhash)
			st, err := state.New(header.Root, db)
			if err == nil {
				bal := st.GetBalance(addr)
				rlp, _ := rlp.EncodeToBytes(bal)
				res = append(res, rlp...)
			}
		} else {
			header := lc.GetHeader(bhash)
			st := light.NewLightState(light.StateTrieID(header), lc.Odr())
			bal, err := st.GetBalance(ctx, addr)
			if err == nil {
				rlp, _ := rlp.EncodeToBytes(bal)
				res = append(res, rlp...)
			}
		}
	}

	return res
}

func TestOdrContractCallLes1(t *testing.T) { testOdr(t, 1, 2, odrContractCall) }

// fullcallmsg is the message type used for call transations.
type fullcallmsg struct {
	from          *state.StateObject
	to            *common.Address
	gas, gasPrice *big.Int
	value         *big.Int
	data          []byte
}

// accessor boilerplate to implement core.Message
func (m fullcallmsg) From() (common.Address, error)         { return m.from.Address(), nil }
func (m fullcallmsg) FromFrontier() (common.Address, error) { return m.from.Address(), nil }
func (m fullcallmsg) Nonce() uint64                         { return m.from.Nonce() }
func (m fullcallmsg) To() *common.Address                   { return m.to }
func (m fullcallmsg) GasPrice() *big.Int                    { return m.gasPrice }
func (m fullcallmsg) Gas() *big.Int                         { return m.gas }
func (m fullcallmsg) Value() *big.Int                       { return m.value }
func (m fullcallmsg) Data() []byte                          { return m.data }

// callmsg is the message type used for call transations.
type lightcallmsg struct {
	from          *light.StateObject
	to            *common.Address
	gas, gasPrice *big.Int
	value         *big.Int
	data          []byte
}

// accessor boilerplate to implement core.Message
func (m lightcallmsg) From() (common.Address, error)         { return m.from.Address(), nil }
func (m lightcallmsg) FromFrontier() (common.Address, error) { return m.from.Address(), nil }
func (m lightcallmsg) Nonce() uint64                         { return m.from.Nonce() }
func (m lightcallmsg) To() *common.Address                   { return m.to }
func (m lightcallmsg) GasPrice() *big.Int                    { return m.gasPrice }
func (m lightcallmsg) Gas() *big.Int                         { return m.gas }
func (m lightcallmsg) Value() *big.Int                       { return m.value }
func (m lightcallmsg) Data() []byte                          { return m.data }

func odrContractCall(ctx context.Context, db ethdb.Database, bc *core.BlockChain, lc *light.LightChain, bhash common.Hash) []byte {
	data := common.Hex2Bytes("60CD26850000000000000000000000000000000000000000000000000000000000000000")

	var res []byte
	for i := 0; i < 3; i++ {
		data[35] = byte(i)
		if bc != nil {
			header := bc.GetHeader(bhash)
			statedb, err := state.New(header.Root, db)
			if err == nil {
				from := statedb.GetOrNewStateObject(testBankAddress)
				from.SetBalance(common.MaxBig)

				msg := fullcallmsg{
					from:     from,
					gas:      big.NewInt(100000),
					gasPrice: big.NewInt(0),
					value:    big.NewInt(0),
					data:     data,
					to:       &testContractAddr,
				}

				vmenv := core.NewEnv(statedb, bc, msg, header)
				gp := new(core.GasPool).AddGas(common.MaxBig)
				ret, _, _ := core.ApplyMessage(vmenv, msg, gp)
				res = append(res, ret...)
			}
		} else {
			header := lc.GetHeader(bhash)
			state := light.NewLightState(light.StateTrieID(header), lc.Odr())
			from, err := state.GetOrNewStateObject(ctx, testBankAddress)
			if err == nil {
				from.SetBalance(common.MaxBig)

				msg := lightcallmsg{
					from:     from,
					gas:      big.NewInt(100000),
					gasPrice: big.NewInt(0),
					value:    big.NewInt(0),
					data:     data,
					to:       &testContractAddr,
				}

				vmenv := light.NewEnv(ctx, state, lc, msg, header)
				gp := new(core.GasPool).AddGas(common.MaxBig)
				ret, _, _ := core.ApplyMessage(vmenv, msg, gp)
				if vmenv.Error() == nil {
					res = append(res, ret...)
				}
			}
		}
	}
	return res
}

func testOdr(t *testing.T, protocol int, expFail uint64, fn odrTestFn) {
	// Assemble the test environment
	pm, db, odr := newTestProtocolManagerMust(t, false, 4, testChainGen)
	lpm, ldb, odr := newTestProtocolManagerMust(t, true, 0, nil)
	_, _, lpeer, _ := newTestPeerPair("peer", protocol, pm, lpm)
	time.Sleep(time.Millisecond * 100)
	lpm.synchronise(lpeer, true)

	test := func(expFail uint64) {
		for i := uint64(0); i <= pm.blockchain.CurrentHeader().GetNumberU64(); i++ {
			bhash := core.GetCanonicalHash(db, i)
			b1 := fn(light.NoOdr, db, pm.blockchain.(*core.BlockChain), nil, bhash)
			ctx, _ := context.WithTimeout(context.Background(), 200*time.Millisecond)
			b2 := fn(ctx, ldb, nil, lpm.blockchain.(*light.LightChain), bhash)
			eq := bytes.Equal(b1, b2)
			exp := i < expFail
			if exp && !eq {
				t.Errorf("odr mismatch")
			}
			if !exp && eq {
				t.Errorf("unexpected odr match")
			}
		}
	}

	// temporarily remove peer to test odr fails
	odr.UnregisterPeer(lpeer.id)
	// expect retrievals to fail (except genesis block) without a les peer
	test(expFail)
	odr.RegisterPeer(lpeer.id, lpeer.version, lpeer.Head(), lpeer.RequestBodies, lpeer.RequestCode, lpeer.RequestReceipts, lpeer.RequestProofs)
	// expect all retrievals to pass
	test(5)
	odr.UnregisterPeer(lpeer.id)
	// still expect all retrievals to pass, now data should be cached locally
	test(5)
}
