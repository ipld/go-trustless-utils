package trustlesshttp_test

import (
	"testing"

	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/stretchr/testify/require"
)

func TestContentType(t *testing.T) {
	req := require.New(t)

	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", trustlesshttp.ResponseContentTypeHeader(true))
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", trustlesshttp.RequestAcceptHeader(true))
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=n", trustlesshttp.ResponseContentTypeHeader(false))
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=n", trustlesshttp.RequestAcceptHeader(false))

	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", trustlesshttp.NewContentType().String())
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y;q=0.8", trustlesshttp.NewContentType(trustlesshttp.WithContentTypeQuality(0.8)).String())
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y;q=0.333", trustlesshttp.NewContentType(trustlesshttp.WithContentTypeQuality(1.0/3.0)).String())
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", trustlesshttp.NewContentType(trustlesshttp.WithContentTypeQuality(-1.0)).String())
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=n", trustlesshttp.NewContentType(trustlesshttp.WithContentTypeDuplicates(false)).String())
	req.Equal("application/vnd.ipld.car;version=1;order=unk;dups=n", trustlesshttp.NewContentType(trustlesshttp.WithContentTypeDuplicates(false), trustlesshttp.WithContentTypeOrder(trustlesshttp.ContentTypeOrderUnk)).String())
}
