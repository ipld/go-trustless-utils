package traversal

import (
	"io"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
)

// ErrorCapturingReader captures any errors that occur during block loading
// and makes them available via the Error property.
//
// This is useful for capturing errors that occur during traversal, which are
// not currently surfaced by the traversal package, see:
//
//	https://github.com/ipld/go-ipld-prime/pull/524
type ErrorCapturingReader struct {
	sro   linking.BlockReadOpener
	Error error
}

func NewErrorCapturingReader(lsys linking.LinkSystem) (linking.LinkSystem, *ErrorCapturingReader) {
	ecr := &ErrorCapturingReader{sro: lsys.StorageReadOpener}
	lsys.StorageReadOpener = ecr.StorageReadOpener
	return lsys, ecr
}

func (ecr *ErrorCapturingReader) StorageReadOpener(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
	r, err := ecr.sro(lc, l)
	if err != nil {
		ecr.Error = err
	}
	return r, err
}
