package testutil

import (
	"testing"

	cid "github.com/ipfs/go-cid"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var djlp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.DagJSON,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

var rawlp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

// MakeDagWithIdentity makes a non-unixfs DAG, wrapped in the
// go-unixfsnode/testutil/DirEntry struct (so it can be used in all the places
// DirEntry is) that has an identity CID in the middle linking a section that
// the identity CID must be traversed through to find.
func MakeDagWithIdentity(t *testing.T, lsys linking.LinkSystem) unixfs.DirEntry {
	/* ugly, but it makes a DAG with paths that look like this but doesn't involved dag-pb or unixfs
		> [/]
	  > [/a/!foo]
	  > [/a/b/!bar]
	  > [/a/b/c/!baz/identity jump]
	    > [/a/b/c/!baz/identity jump/these are my children/blip]
	    > [/a/b/c/!baz/identity jump/these are my children/bloop]
	      > [/a/b/c/!baz/identity jump/these are my children/bloop/  leaf  ]
	    > [/a/b/c/!baz/identity jump/these are my children/blop]
	    > [/a/b/c/!baz/identity jump/these are my children/leaf]
	  > [/a/b/c/d/!leaf]
	*/
	store := func(path string, children []unixfs.DirEntry, lp cidlink.LinkPrototype, n datamodel.Node) unixfs.DirEntry {
		l, err := lsys.Store(linking.LinkContext{}, lp, n)
		require.NoError(t, err)
		var content []byte
		if n.Kind() == datamodel.Kind_Bytes {
			content, err = n.AsBytes()
			require.NoError(t, err)
		}
		return unixfs.DirEntry{
			Path:     path,
			Root:     l.(cidlink.Link).Cid,
			SelfCids: []cid.Cid{l.(cidlink.Link).Cid},
			Children: children,
			Content:  content,
		}
	}

	bazpath := "/a/b/c/!baz/identity jump"
	bazchildpath := "/these are my children"
	bloppath := "/bloop"

	children := []unixfs.DirEntry{store(bazpath+bazchildpath+bloppath+"/  leaf  ", nil, rawlp, basicnode.NewBytes([]byte("leaf node in bloop")))}
	bloop := store(bazpath+bazchildpath+"/bloop", children, djlp, must(qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "desc", qp.List(-1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.String("this"))
			qp.ListEntry(la, qp.String("is"))
			qp.ListEntry(la, qp.String("bloop"))
		}))
		qp.MapEntry(ma, "  leaf  ", qp.Link(cidlink.Link{Cid: children[0].Root}))
	}))(t))
	leaf := store(bazpath+bazchildpath+"/leaf", nil, rawlp, basicnode.NewBytes([]byte("leaf node in baz")))
	blop := store(bazpath+bazchildpath+"/blop", nil, djlp, must(qp.BuildList(basicnode.Prototype.Any, -1, func(la datamodel.ListAssembler) {
		qp.ListEntry(la, qp.Int(100))
		qp.ListEntry(la, qp.Int(200))
		qp.ListEntry(la, qp.Int(300))
	}))(t))
	blip := store(bazpath+bazchildpath+"/blip", nil, djlp, basicnode.NewString("blip!"))
	baz := store(bazpath, []unixfs.DirEntry{blip, bloop, blop, leaf}, djlp, must(qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "desc", qp.List(-1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.String("this"))
			qp.ListEntry(la, qp.String("is"))
			qp.ListEntry(la, qp.String("baz"))
		}))
		qp.MapEntry(ma, "these are my children", qp.Map(-1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "blip", qp.Link(cidlink.Link{Cid: blip.Root}))
			qp.MapEntry(ma, "bloop", qp.Link(cidlink.Link{Cid: bloop.Root}))
			qp.MapEntry(ma, "blop", qp.Link(cidlink.Link{Cid: blop.Root}))
			qp.MapEntry(ma, "leaf", qp.Link(cidlink.Link{Cid: leaf.Root}))
		}))
	}))(t))
	leaf = store("/a/b/c/d/!leaf", nil, rawlp, basicnode.NewBytes([]byte("leaf node in the root")))
	foo := store("/a/!foo", nil, djlp, basicnode.NewInt(1010101010101010))
	bar := store("/a/b/!bar", nil, djlp, basicnode.NewInt(2020202020202020))
	ident := must(qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "identity jump", qp.Link(cidlink.Link{Cid: baz.Root}))
	}))(t)
	identBytes := must(ipld.Encode(ident, dagjson.Encode))(t)
	mh := must(multihash.Sum(identBytes, multihash.IDENTITY, len(identBytes)))(t)
	bazident := cid.NewCidV1(cid.DagJSON, mh)
	bazidentChild := unixfs.DirEntry{
		Root:     bazident,
		Path:     "/a/b/c/!baz",
		Children: []unixfs.DirEntry{baz},
	}
	root := store("", []unixfs.DirEntry{foo, bar, bazidentChild, leaf}, djlp, must(qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "a", qp.Map(-1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "b", qp.Map(-1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "c", qp.Map(-1, func(ma datamodel.MapAssembler) {
					qp.MapEntry(ma, "d", qp.Map(-1, func(ma datamodel.MapAssembler) {
						qp.MapEntry(ma, "!leaf", qp.Link(cidlink.Link{Cid: leaf.Root}))
					}))
					qp.MapEntry(ma, "!baz", qp.Link(cidlink.Link{Cid: bazident}))
				}))
				qp.MapEntry(ma, "!bar", qp.Link(cidlink.Link{Cid: bar.Root}))
			}))
			qp.MapEntry(ma, "!foo", qp.Link(cidlink.Link{Cid: foo.Root}))
		}))
	}))(t))
	return root
}

func must[T any](v T, err error) func(t *testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}
