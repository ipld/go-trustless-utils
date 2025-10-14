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
	carAccepts := []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}
	rawAccepts := []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}
	bothAccepts := []trustlesshttp.ContentType{trustlesshttp.DefaultContentType(), trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}

	for _, tc := range []struct {
		name     string
		query    string
		accepts  []trustlesshttp.ContentType
		expected string
		err      string
	}{
		{"no filename", "", carAccepts, "", ""},
		{"boop.car with CAR accept", "filename=boop.car", carAccepts, "boop.car", ""},
		{"boop.bin with raw accept", "filename=boop.bin", rawAccepts, "boop.bin", ""},
		{"boop.car with both accepts", "filename=boop.car", bothAccepts, "boop.car", ""},
		{"boop.bin with both accepts", "filename=boop.bin", bothAccepts, "boop.bin", ""},
		{"blank (err)", "filename=", carAccepts, "", "invalid filename parameter; missing extension"},
		{"no extension (err)", "filename=bork", carAccepts, "", "invalid filename parameter; missing extension"},
		{"bad extension (err)", "filename=bork.exe", carAccepts, "", "invalid filename parameter; unsupported extension: \".exe\""},
		{".car with raw accept (err)", "filename=boop.car", rawAccepts, "", ".car extension requires CAR response format"},
		{".bin with CAR accept (err)", "filename=boop.bin", carAccepts, "", ".bin extension requires raw response format"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{}
			req.URL = &url.URL{RawQuery: tc.query}
			filename, err := trustlesshttp.ParseFilename(req, tc.accepts)
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
		name         string
		accept       string
		query        string
		expectAccept []trustlesshttp.ContentType
		err          string
	}{
		{"empty (err)", "", "", []trustlesshttp.ContentType{{}}, "neither a valid Accept header nor format parameter were provided"},
		{"format=bop (err)", "", "format=bop", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, "invalid format parameter; unsupported: \"bop\""},
		{"format=car", "", "format=car", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, ""},
		{"format=raw", "", "format=raw", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}, ""},
		{"car accept", "application/vnd.ipld.car", "", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, ""},
		{"raw accept", "application/vnd.ipld.raw", "", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}, ""},
		{"raw accept plus garbage", "application/vnd.ipld.raw; ignore; this", "", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}, ""},
		{"accept dups", "application/vnd.ipld.car; dups=y", "", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, ""},
		{"accept no dups", "application/vnd.ipld.car; dups=n", "", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithDuplicates(false)}, ""},
		{"accept no dups and cruft", "application/vnd.ipld.car; dups=n; bip; bop", "", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithDuplicates(false)}, ""},
		{"valid accept but format=bop (err)", "application/vnd.ipld.car; dups=y", "format=bop", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, "invalid format parameter; unsupported: \"bop\""},
		{"specific accept car with format=car (accept wins per spec)", "application/vnd.ipld.car; dups=y", "format=car", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, ""},
		{"specific accept car with format=raw (accept wins per spec)", "application/vnd.ipld.car; dups=n", "format=raw", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithDuplicates(false)}, ""},
		{"specific accept raw with format=car (accept wins per spec)", "application/vnd.ipld.raw", "format=car", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}, ""},
		{"invalid accept but format=car (format wins)", "application/vnd.ipld.car; dups=YES!", "format=car", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, ""},
		{"invalid accept but format=raw (format wins)", "application/vnd.ipld.car; dups=YES!", "format=raw", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}, ""},
		{"wildcard */* with format=raw (format wins)", "*/*", "format=raw", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}, ""},
		{"wildcard */* with format=car (format wins)", "*/*", "format=car", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, ""},
		{"wildcard application/* with format=raw (format wins)", "application/*", "format=raw", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)}, ""},
		{"wildcard application/* with format=car (format wins)", "application/*", "format=car", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType()}, ""},
		{"ordered, valid", "application/vnd.ipld.raw, application/*, application/vnd.ipld.car; dups=y", "", []trustlesshttp.ContentType{trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw), trustlesshttp.DefaultContentType().WithMimeType("application/*"), trustlesshttp.DefaultContentType().WithDuplicates(true)}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{}
			req.URL = &url.URL{RawQuery: tc.query}
			if tc.accept != "" {
				req.Header = http.Header{"Accept": []string{tc.accept}}
			}
			accept, err := trustlesshttp.CheckFormat(req)
			if tc.err == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectAccept, accept)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			}
		})
	}
}

func TestParseContentType(t *testing.T) {
	for _, tc := range []struct {
		name              string
		accept            string
		expectValid       bool
		expectContentType trustlesshttp.ContentType
	}{
		{"empty (err)", "", false, trustlesshttp.ContentType{}},
		{"car", "application/vnd.ipld.car", true, trustlesshttp.DefaultContentType()},
		{"raw", "application/vnd.ipld.raw", true, trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)},
		{"*/*", "*/*", false, trustlesshttp.ContentType{}},
		{"application/*", "application/*", false, trustlesshttp.ContentType{}},
		{"dups", "application/vnd.ipld.car; dups=y", true, trustlesshttp.DefaultContentType()},
		{"no dups", "application/vnd.ipld.car; dups=n", true, trustlesshttp.DefaultContentType().WithDuplicates(false)},
		{"no dups and cruft", "application/vnd.ipld.car; dups=n; bip; bop", true, trustlesshttp.DefaultContentType().WithDuplicates(false)},
		{"raw and cruft", "application/vnd.ipld.raw; bip; bop", true, trustlesshttp.DefaultContentType().WithMimeType(trustlesshttp.MimeTypeRaw)},
		{"version=1", "application/vnd.ipld.car; version=1; dups=n", true, trustlesshttp.DefaultContentType().WithDuplicates(false)},
		{"version=2", "application/vnd.ipld.car; version=2; dups=n", false, trustlesshttp.ContentType{}},
		{"order=dfs", "application/vnd.ipld.car; order=dfs; dups=n", true, trustlesshttp.DefaultContentType().WithDuplicates(false)},
		{"order=unk", "application/vnd.ipld.car; order=unk; dups=n", true, trustlesshttp.DefaultContentType().WithDuplicates(false).WithOrder(trustlesshttp.ContentTypeOrderUnk)},
		{"order=bork", "application/vnd.ipld.car; order=bork; dups=y", false, trustlesshttp.ContentType{}},
		{"complete", "application/vnd.ipld.car; order=dfs; dups=y; version=1", true, trustlesshttp.DefaultContentType()},
		{"complete (squish)", "application/vnd.ipld.car;order=dfs;dups=y;version=1", true, trustlesshttp.DefaultContentType()},
		{"complete (shuffle)", "application/vnd.ipld.car;version=1;dups=y;order=dfs;", true, trustlesshttp.DefaultContentType()},
		{"complete (cruft)", "application/vnd.ipld.car;;version=1; bip ;   dups=n ;bop;order=dfs;--", true, trustlesshttp.DefaultContentType().WithDuplicates(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ct, valid := trustlesshttp.ParseContentType(tc.accept)
			require.Equal(t, tc.expectValid, valid)
			require.Equal(t, tc.expectContentType, ct)
		})
	}
}

func TestParseAccept(t *testing.T) {
	for _, tc := range []struct {
		name     string
		accept   string
		expected []trustlesshttp.ContentType
	}{
		{"empty (err)", "", []trustlesshttp.ContentType{}},
		{"plain", "application/vnd.ipld.car", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"*/*", "*/*", []trustlesshttp.ContentType{{MimeType: "*/*", Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"application/*", "application/*", []trustlesshttp.ContentType{{MimeType: "application/*", Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"dups", "application/vnd.ipld.car; dups=y", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"no dups", "application/vnd.ipld.car; dups=n", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: false, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"no dups and cruft", "application/vnd.ipld.car; dups=n; bip; bop", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: false, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"version=1", "application/vnd.ipld.car; version=1; dups=n", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: false, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"version=2", "application/vnd.ipld.car; version=2; dups=n", []trustlesshttp.ContentType{}},
		{"order=dfs", "application/vnd.ipld.car; order=dfs; dups=n", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: false, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"order=unk", "application/vnd.ipld.car; order=unk; dups=n", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: false, Order: trustlesshttp.ContentTypeOrderUnk, Quality: 1.0}}},
		{"order=bork", "application/vnd.ipld.car; order=bork; dups=y", []trustlesshttp.ContentType{}},
		{"complete", "application/vnd.ipld.car; order=dfs; dups=y; version=1", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"complete (squish)", "application/vnd.ipld.car;order=dfs;dups=y;version=1", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"complete (shuffle)", "application/vnd.ipld.car;version=1;dups=y;order=dfs;", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"complete (cruft)", "application/vnd.ipld.car;;version=1; bip ;   dups=n ;bop;order=dfs;--", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: false, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0}}},
		{"q", "application/vnd.ipld.car; order=dfs; q=0.77; dups=n", []trustlesshttp.ContentType{{MimeType: trustlesshttp.MimeTypeCar, Duplicates: false, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 0.77}}},
		{"q=bork", "application/vnd.ipld.car; order=dfs; q=bork; dups=n", []trustlesshttp.ContentType{}},
		{"q=-1", "application/vnd.ipld.car; order=dfs; q=-0.1; dups=n", []trustlesshttp.ContentType{}},

		{
			"ordered",
			"application/vnd.ipld.car;dups=n;order=unk;q=0.8, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.1, application/vnd.ipld.car;dups=y;order=dfs;q=0.9 , application/vnd.ipld.car, application/vnd.ipld.raw,application/vnd.ipld.raw;q=0.1, application/vnd.ipld.car;dups=y;order=unk;q=0.7, application/vnd.ipld.car;dups=y;order=dfs;q=0.7",
			[]trustlesshttp.ContentType{
				{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0},
				{MimeType: trustlesshttp.MimeTypeRaw, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 1.0},
				{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 0.9},
				{MimeType: trustlesshttp.MimeTypeCar, Duplicates: false, Order: trustlesshttp.ContentTypeOrderUnk, Quality: 0.8},
				{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderUnk, Quality: 0.7},
				{MimeType: trustlesshttp.MimeTypeCar, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 0.7},
				{MimeType: "*/*", Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 0.1},
				{MimeType: trustlesshttp.MimeTypeRaw, Duplicates: true, Order: trustlesshttp.ContentTypeOrderDfs, Quality: 0.1},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accepts := trustlesshttp.ParseAccept(tc.accept)
			require.Equal(t, tc.expected, accepts)
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
