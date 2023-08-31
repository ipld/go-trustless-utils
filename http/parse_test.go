package trustlesshttp_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/ipfs/go-cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	"github.com/stretchr/testify/require"
)

var testCidV1 = cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

func TestParseScope(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		expected trustlessutils.DagScope
		err      string
	}{
		{"no query", "", trustlessutils.DagScopeAll, ""},
		{"all", "dag-scope=all", trustlessutils.DagScopeAll, ""},
		{"entity", "dag-scope=entity", trustlessutils.DagScopeEntity, ""},
		{"block", "dag-scope=block", trustlessutils.DagScopeBlock, ""},
		{"bork", "dag-scope=bork", "", "invalid dag-scope parameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{}
			req.URL = &url.URL{RawQuery: tc.query}
			ds, err := trustlesshttp.ParseScope(req)
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ds)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			}
		})
	}
}

func TestByteRange(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		expected *trustlessutils.ByteRange
		err      string
	}{
		{"no query", "", nil, ""},
		{"0:0", "entity-bytes=0:0", &trustlessutils.ByteRange{From: 0, To: ptr(int64(0))}, ""},
		{"0:*", "entity-bytes=0:*", &trustlessutils.ByteRange{From: 0}, ""},
		{"101:*", "entity-bytes=101:*", &trustlessutils.ByteRange{From: 101}, ""},
		{"101:202", "entity-bytes=101:202", &trustlessutils.ByteRange{From: 101, To: ptr(int64(202))}, ""},
		{"-101:-202", "entity-bytes=-101:-202", &trustlessutils.ByteRange{From: -101, To: ptr(int64(-202))}, ""},
		{"0 (err)", "entity-bytes=0", nil, "invalid entity-bytes parameter"},
		{"bork (err)", "entity-bytes=bork", nil, "invalid entity-bytes parameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{}
			req.URL = &url.URL{RawQuery: tc.query}
			br, err := trustlesshttp.ParseByteRange(req)
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, br)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			}
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}

func TestParseFilename(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		expected string
		err      string
	}{
		{"no filename", "", "", ""},
		{"boop.car", "filename=boop.car", "boop.car", ""},
		{"blank (err)", "filename=", "", "invalid filename parameter; missing extension"},
		{"no .car (err)", "filename=bork", "", "invalid filename parameter; missing extension"},
		{"bad extensio (err)", "filename=bork.exe", "", "invalid filename parameter; unsupported extension: \".exe\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{}
			req.URL = &url.URL{RawQuery: tc.query}
			filename, err := trustlesshttp.ParseFilename(req)
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, filename)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			}
		})
	}
}

func TestCheckFormat(t *testing.T) {
	for _, tc := range []struct {
		name       string
		accept     string
		query      string
		expectDups bool
		err        string
	}{
		{"empty (err)", "", "", false, "neither a valid Accept header nor format parameter were provided"},
		{"format=bop (err)", "", "format=bop", true, "invalid format parameter; unsupported: \"bop\""},
		{"format=car", "", "format=car", true, ""},
		{"plain accept", "application/vnd.ipld.car", "", true, ""},
		{"accept dups", "application/vnd.ipld.car; dups=y", "", true, ""},
		{"accept no dups", "application/vnd.ipld.car; dups=n", "", false, ""},
		{"accept no dups and cruft", "application/vnd.ipld.car; dups=n; bip; bop", "", false, ""},
		{"valid accept but format=bop (err)", "application/vnd.ipld.car; dups=y", "format=bop", true, "invalid format parameter; unsupported: \"bop\""},
		{"valid accept but format=car", "application/vnd.ipld.car; dups=y", "format=car", true, ""},
		{"invalid accept but format=car", "application/vnd.ipld.car; dups=YES!", "format=car", false, "invalid Accept header; unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{}
			req.URL = &url.URL{RawQuery: tc.query}
			if tc.accept != "" {
				req.Header = http.Header{"Accept": []string{tc.accept}}
			}
			dups, err := trustlesshttp.CheckFormat(req)
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectDups, dups)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			}
		})
	}
}

func TestParseAcceptAndContentType(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		accept                 string
		expectValidAccept      bool
		expectValidContentType bool
		expectDups             bool
	}{
		{"empty (err)", "", false, false, true},
		{"plain", "application/vnd.ipld.car", true, true, true},
		{"*/*", "*/*", true, false, true},
		{"application/*", "application/*", true, false, true},
		{"dups", "application/vnd.ipld.car; dups=y", true, true, true},
		{"no dups", "application/vnd.ipld.car; dups=n", true, true, false},
		{"no dups and cruft", "application/vnd.ipld.car; dups=n; bip; bop", true, true, false},
		{"version=1", "application/vnd.ipld.car; version=1; dups=n", true, true, false},
		{"version=2", "application/vnd.ipld.car; version=2; dups=n", false, false, false},
		{"order=dfs", "application/vnd.ipld.car; order=dfs; dups=n", true, true, false},
		{"order=unk", "application/vnd.ipld.car; order=unk; dups=n", true, true, false},
		{"order=bork", "application/vnd.ipld.car; order=bork; dups=y", false, false, true},
		{"complete", "application/vnd.ipld.car; order=dfs; dups=y; version=1", true, true, true},
		{"complete (squish)", "application/vnd.ipld.car;order=dfs;dups=y;version=1", true, true, true},
		{"complete (shuffle)", "application/vnd.ipld.car;version=1;dups=y;order=dfs;", true, true, true},
		{"complete (cruft)", "application/vnd.ipld.car;;version=1; bip ;   dups=n ;bop;order=dfs;--", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valid, dups := trustlesshttp.ParseAccept(tc.accept)
			require.Equal(t, tc.expectValidAccept, valid)
			require.Equal(t, tc.expectDups, dups)

			valid, dups = trustlesshttp.ParseContentType(tc.accept)
			require.Equal(t, tc.expectValidContentType, valid)
			require.Equal(t, tc.expectDups, dups)
		})
	}
}

func TestParseUrlPath(t *testing.T) {
	for _, tc := range []struct {
		name         string
		path         string
		expectedRoot cid.Cid
		expectedPath string
		err          string
	}{
		{"empty (err)", "", cid.Undef, "", "not found"},
		{"slash (err)", "/", cid.Undef, "", "not found"},
		{"no ipfs pfx (err)", "/ipld", cid.Undef, "", "not found"},
		{"no cid (err)", "/ipfs", cid.Undef, "", "not found"},
		{"no cid 2 (err)", "/ipfs/", cid.Undef, "", "not found"},
		{"bad (err)", "/ipfs/nope", cid.Undef, "", "failed to parse root CID"},
		{"bad 2 (err)", "/ipfs/bafyfoo", cid.Undef, "", "failed to parse root CID"},
		{"just root", "/ipfs/" + testCidV1.String(), testCidV1, "", ""},
		{"just root and slash", "/ipfs/" + testCidV1.String() + "/", testCidV1, "", ""},
		{"just root and slashes", "/ipfs/" + testCidV1.String() + "///", testCidV1, "", ""},
		{"root and path", "/ipfs/" + testCidV1.String() + "/foo/bar", testCidV1, "foo/bar", ""},
		{"root and path and slashes", "/ipfs/" + testCidV1.String() + "//foo//bar///", testCidV1, "foo/bar", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, path, err := trustlesshttp.ParseUrlPath(tc.path)
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedRoot, root)
				require.Equal(t, tc.expectedPath, path.String())
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			}
		})
	}
}
