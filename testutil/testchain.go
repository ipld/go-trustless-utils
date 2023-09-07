package testutil

import (
	"bytes"
	"context"
	"io"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipld/go-trustless-utils/testutil/chaintypes"
	"github.com/jbenet/go-random"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// TestBlockChain is a simulated data structure similar to a blockchain
type TestBlockChain struct {
	t                testing.TB
	blockChainLength int
	loader           ipld.BlockReadOpener
	GenisisNode      ipld.Node
	GenisisLink      ipld.Link
	MiddleNodes      []ipld.Node
	MiddleLinks      []ipld.Link
	TipNode          ipld.Node
	TipLink          ipld.Link
}

func createBlock(parents []ipld.Link, size uint64) (ipld.Node, error) {
	blknb := chaintypes.Type.Block.NewBuilder()
	blknbmnb, err := blknb.BeginMap(2)
	if err != nil {
		return nil, err
	}

	entnb, err := blknbmnb.AssembleEntry("Parents")
	if err != nil {
		return nil, err
	}
	pnblnb, err := entnb.BeginList(int64(len(parents)))
	if err != nil {
		return nil, err
	}
	for _, parent := range parents {
		err := pnblnb.AssembleValue().AssignLink(parent)
		if err != nil {
			return nil, err
		}
	}
	err = pnblnb.Finish()
	if err != nil {
		return nil, err
	}

	entnb, err = blknbmnb.AssembleEntry("Messages")
	if err != nil {
		return nil, err
	}
	mnblnb, err := entnb.BeginList(1)
	if err != nil {
		return nil, err
	}
	err = mnblnb.AssembleValue().AssignBytes(RandomBytes(int64(size)))
	if err != nil {
		return nil, err
	}
	err = mnblnb.Finish()
	if err != nil {
		return nil, err
	}

	err = blknbmnb.Finish()
	if err != nil {
		return nil, err
	}
	return blknb.Build(), nil
}

// SetupBlockChain creates a new test block chain with the given height
func SetupBlockChain(
	ctx context.Context,
	t testing.TB,
	lsys ipld.LinkSystem,
	size uint64,
	blockChainLength int) *TestBlockChain {
	linkPrototype := cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(cid.DagCBOR, mh.SHA2_256)}
	genisisNode, err := createBlock([]ipld.Link{}, size)
	require.NoError(t, err, "Error creating genesis block")
	genesisLink, err := lsys.Store(ipld.LinkContext{Ctx: ctx}, linkPrototype, genisisNode)
	require.NoError(t, err, "Error creating link to genesis block")
	parent := genesisLink
	middleNodes := make([]ipld.Node, 0, blockChainLength-2)
	middleLinks := make([]ipld.Link, 0, blockChainLength-2)
	for i := 0; i < blockChainLength-2; i++ {
		node, err := createBlock([]ipld.Link{parent}, size)
		require.NoError(t, err, "Error creating middle block")
		middleNodes = append(middleNodes, node)
		link, err := lsys.Store(ipld.LinkContext{Ctx: ctx}, linkPrototype, node)
		require.NoError(t, err, "Error creating link to middle block")
		middleLinks = append(middleLinks, link)
		parent = link
	}
	tipNode, err := createBlock([]ipld.Link{parent}, size)
	require.NoError(t, err, "Error creating tip block")
	tipLink, err := lsys.Store(ipld.LinkContext{Ctx: ctx}, linkPrototype, tipNode)
	require.NoError(t, err, "Error creating link to tip block")
	return &TestBlockChain{t, blockChainLength, lsys.StorageReadOpener, genisisNode, genesisLink, middleNodes, middleLinks, tipNode, tipLink}
}

// Selector returns the selector to recursive traverse the block chain parent links
func (tbc *TestBlockChain) Selector() ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(selector.RecursionLimitDepth(int64(tbc.blockChainLength)),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Parents", ssb.ExploreAll(
				ssb.ExploreRecursiveEdge()))
		})).Node()
}

// LinkTipIndex returns a link to the block at the given index from the tip
func (tbc *TestBlockChain) LinkTipIndex(fromTip int) ipld.Link {
	switch height := tbc.blockChainLength - 1 - fromTip; {
	case height == 0:
		return tbc.GenisisLink
	case height == tbc.blockChainLength-1:
		return tbc.TipLink
	default:
		return tbc.MiddleLinks[height-1]
	}
}

// NodeTipIndex returns the node to the block at the given index from the tip
func (tbc *TestBlockChain) NodeTipIndex(fromTip int) ipld.Node {
	switch height := tbc.blockChainLength - 1 - fromTip; {
	case height == 0:
		return tbc.GenisisNode
	case height == tbc.blockChainLength-1:
		return tbc.TipNode
	default:
		return tbc.MiddleNodes[height-1]
	}
}

// PathTipIndex returns the path to the block at the given index from the tip
func (tbc *TestBlockChain) PathTipIndex(fromTip int) ipld.Path {
	expectedPath := make([]datamodel.PathSegment, 0, 2*fromTip)
	for i := 0; i < fromTip; i++ {
		expectedPath = append(expectedPath, datamodel.PathSegmentOfString("Parents"), datamodel.PathSegmentOfInt(0))
	}
	return datamodel.NewPath(expectedPath)
}

// Blocks Returns the given raw blocks for the block chain for the given range, indexed from the tip
func (tbc *TestBlockChain) Blocks(from int, to int) []blocks.Block {
	var blks []blocks.Block
	for i := from; i < to; i++ {
		link := tbc.LinkTipIndex(i)
		reader, err := tbc.loader(ipld.LinkContext{}, link)
		require.NoError(tbc.t, err)
		data, err := io.ReadAll(reader)
		require.NoError(tbc.t, err)
		blk, err := blocks.NewBlockWithCid(data, link.(cidlink.Link).Cid)
		require.NoError(tbc.t, err)
		blks = append(blks, blk)
	}
	return blks
}

// AllBlocks returns all blocks for a blockchain
func (tbc *TestBlockChain) AllBlocks() []blocks.Block {
	return tbc.Blocks(0, tbc.blockChainLength)
}

// RemainderBlocks returns the remaining blocks for a blockchain, indexed from tip
func (tbc *TestBlockChain) RemainderBlocks(from int) []blocks.Block {
	return tbc.Blocks(from, tbc.blockChainLength)
}

// Chooser is a NodeBuilderChooser function that always returns the block chain
func (tbc *TestBlockChain) Chooser(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
	return chaintypes.Type.Block, nil
}

var seedSeq int64

// RandomBytes returns a byte array of the given size with random values.
func RandomBytes(n int64) []byte {
	data := new(bytes.Buffer)
	_ = random.WritePseudoRandomBytes(n, data, seedSeq)
	seedSeq++
	return data.Bytes()
}
