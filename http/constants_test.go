package trustlesshttp_test

import (
	"testing"

	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/stretchr/testify/require"
)

func TestContentType(t *testing.T) {
	require.Equal(t, "application/vnd.ipld.car; version=1; order=dfs; dups=y", trustlesshttp.ResponseContentTypeHeader(true))
	require.Equal(t, "application/vnd.ipld.car; version=1; order=dfs; dups=y", trustlesshttp.RequestAcceptHeader(true))
	require.Equal(t, "application/vnd.ipld.car; version=1; order=dfs; dups=n", trustlesshttp.ResponseContentTypeHeader(false))
	require.Equal(t, "application/vnd.ipld.car; version=1; order=dfs; dups=n", trustlesshttp.RequestAcceptHeader(false))
}
