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

	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", trustlesshttp.DefaultContentType().String())
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y;q=0.8", trustlesshttp.DefaultContentType().WithQuality(0.8).String())
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y;q=0.333", trustlesshttp.DefaultContentType().WithQuality(1.0/3.0).String())
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", trustlesshttp.DefaultContentType().WithQuality(-1.0).String())
	req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=n", trustlesshttp.DefaultContentType().WithDuplicates(false).String())
	req.Equal("application/vnd.ipld.car;version=1;order=unk;dups=n", trustlesshttp.DefaultContentType().WithDuplicates(false).WithOrder(trustlesshttp.ContentTypeOrderUnk).String())
}

func TestContentLocation(t *testing.T) {
	testCases := []struct {
		name        string
		contentType trustlesshttp.ContentType
		requestURL  string
		expected    string
	}{
		{
			name:        "CAR without format param",
			contentType: trustlesshttp.DefaultContentType(),
			requestURL:  "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			expected:    "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?format=car",
		},
		{
			name:        "raw without format param",
			contentType: trustlesshttp.ContentType{MimeType: trustlesshttp.MimeTypeRaw},
			requestURL:  "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			expected:    "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?format=raw",
		},
		{
			name:        "CAR with existing query params",
			contentType: trustlesshttp.DefaultContentType(),
			requestURL:  "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=entity",
			expected:    "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=entity&format=car",
		},
		{
			name:        "raw with existing query params",
			contentType: trustlesshttp.ContentType{MimeType: trustlesshttp.MimeTypeRaw},
			requestURL:  "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=block",
			expected:    "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=block&format=raw",
		},
		{
			name:        "already has format param - no Content-Location",
			contentType: trustlesshttp.DefaultContentType(),
			requestURL:  "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?format=car",
			expected:    "",
		},
		{
			name:        "already has format param with other params - no Content-Location",
			contentType: trustlesshttp.ContentType{MimeType: trustlesshttp.MimeTypeRaw},
			requestURL:  "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=entity&format=raw",
			expected:    "",
		},
		{
			name:        "with path",
			contentType: trustlesshttp.DefaultContentType(),
			requestURL:  "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/path/to/file",
			expected:    "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/path/to/file?format=car",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.contentType.ContentLocation(tc.requestURL)
			require.Equal(t, tc.expected, actual)
		})
	}
}
