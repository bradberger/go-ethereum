package les

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/light"
	"golang.org/x/net/context"
)

var testBankSecureTrieKey = secAddr(testBankAddress)

func secAddr(addr common.Address) []byte {
	return crypto.Keccak256(addr[:])
}

type accessTestFn func(db ethdb.Database, bhash common.Hash) light.OdrRequest

func TestBlockAccessLes1(t *testing.T) { testAccess(t, 1, tfBlockAccess) }

func tfBlockAccess(db ethdb.Database, bhash common.Hash) light.OdrRequest {
	return &light.BlockRequest{Hash: bhash}
}

func TestReceiptsAccessLes1(t *testing.T) { testAccess(t, 1, tfReceiptsAccess) }

func tfReceiptsAccess(db ethdb.Database, bhash common.Hash) light.OdrRequest {
	return &light.ReceiptsRequest{Hash: bhash}
}

func TestTrieEntryAccessLes1(t *testing.T) { testAccess(t, 1, tfTrieEntryAccess) }

func tfTrieEntryAccess(db ethdb.Database, bhash common.Hash) light.OdrRequest {
	return &light.TrieRequest{Id: light.StateTrieID(core.GetHeader(db, bhash)), Key: testBankSecureTrieKey}
}

func TestCodeAccessLes1(t *testing.T) { testAccess(t, 1, tfCodeAccess) }

func tfCodeAccess(db ethdb.Database, bhash common.Hash) light.OdrRequest {
	header := core.GetHeader(db, bhash)
	if header.GetNumberU64() < testContractDeployed {
		return nil
	}
	sti := light.StateTrieID(header)
	ci := light.StorageTrieID(sti, testContractAddr, common.Hash{})
	return &light.CodeRequest{Id: ci, Hash: crypto.Keccak256Hash(testContractCodeDeployed)}
}

func testAccess(t *testing.T, protocol int, fn accessTestFn) {
	// Assemble the test environment
	pm, db, _ := newTestProtocolManagerMust(t, false, 4, testChainGen)
	lpm, ldb, odr := newTestProtocolManagerMust(t, true, 0, nil)
	_, _, lpeer, _ := newTestPeerPair("peer", protocol, pm, lpm)
	time.Sleep(time.Millisecond * 100)
	lpm.synchronise(lpeer, true)

	test := func(expFail uint64) {
		for i := uint64(0); i <= pm.blockchain.CurrentHeader().GetNumberU64(); i++ {
			bhash := core.GetCanonicalHash(db, i)
			if req := fn(ldb, bhash); req != nil {
				ctx, _ := context.WithTimeout(context.Background(), 200*time.Millisecond)
				err := odr.Retrieve(ctx, req)
				got := err == nil
				exp := i < expFail
				if exp && !got {
					t.Errorf("object retrieval failed")
				}
				if !exp && got {
					t.Errorf("unexpected object retrieval success")
				}
			}
		}
	}

	// temporarily remove peer to test odr fails
	odr.UnregisterPeer(lpeer.id)
	// expect retrievals to fail (except genesis block) without a les peer
	test(0)
	odr.RegisterPeer(lpeer.id, lpeer.version, lpeer.Head(), lpeer.RequestBodies, lpeer.RequestCode, lpeer.RequestReceipts, lpeer.RequestProofs)
	// expect all retrievals to pass
	test(5)
	odr.UnregisterPeer(lpeer.id)
}
