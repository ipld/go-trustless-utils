package testutil

import (
	"bytes"
	"context"
	"io"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/storage"
	"github.com/multiformats/go-multihash"
)

type ParentStore interface {
	storage.ReadableStorage
	storage.StreamingReadableStorage
	storage.WritableStorage
}

// TODO: remove when this is fixed in IPLD prime
type CorrectedMemStore struct {
	ParentStore
}

func AsIdentity(key string) (digest []byte, ok bool, err error) {
	keyCid, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, false, err
	}
	dmh, err := multihash.Decode(keyCid.Hash())
	if err != nil {
		return nil, false, err
	}
	ok = dmh.Code == multihash.IDENTITY
	digest = dmh.Digest
	return digest, ok, nil
}

func (cms *CorrectedMemStore) Get(ctx context.Context, key string) ([]byte, error) {
	if digest, ok, err := AsIdentity(key); ok {
		return digest, nil
	} else if err != nil {
		return nil, err
	}

	data, err := cms.ParentStore.Get(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return data, err
}

func (cms *CorrectedMemStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if digest, ok, err := AsIdentity(key); ok {
		return io.NopCloser(bytes.NewReader(digest)), nil
	} else if err != nil {
		return nil, err
	}
	rc, err := cms.ParentStore.GetStream(ctx, key)
	if err != nil && err.Error() == "404" {
		err = format.ErrNotFound{}
	}
	return rc, err
}
