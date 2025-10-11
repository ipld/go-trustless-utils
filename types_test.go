package trustlessutils_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/stretchr/testify/require"
)

var testCidV1 = cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
var testCidV0 = cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK")

func TestParseDagScope(t *testing.T) {
	for _, tc := range []struct {
		scope string
		err   string
	}{
		{scope: "all"},
		{scope: "entity"},
		{scope: "block"},
		{scope: "ALL", err: "invalid DagScope: \"ALL\""},
		{scope: "", err: "invalid DagScope: \"\""},
	} {
		t.Run(tc.scope, func(t *testing.T) {
			actual, err := trustlessutils.ParseDagScope(tc.scope)
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.scope, string(actual))
		})
	}
}

func TestDagScopeSelector(t *testing.T) {
	require.Equal(t, unixfsnode.ExploreAllRecursivelySelector, trustlessutils.DagScopeAll.TerminalSelectorSpec())
	require.Equal(t, unixfsnode.MatchUnixFSEntitySelector, trustlessutils.DagScopeEntity.TerminalSelectorSpec())
	require.Equal(t, builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher(), trustlessutils.DagScopeBlock.TerminalSelectorSpec())
	require.Equal(t, unixfsnode.ExploreAllRecursivelySelector, trustlessutils.DagScope("").TerminalSelectorSpec())
}

func TestParseByteRange(t *testing.T) {
	for _, tc := range []struct {
		input    string
		expected trustlessutils.ByteRange
		err      string
	}{
		{"", trustlessutils.ByteRange{}, ""},
		{"0:0", trustlessutils.ByteRange{From: 0, To: ptr(int64(0))}, ""},
		{"0:*", trustlessutils.ByteRange{From: 0}, ""},
		{"101:*", trustlessutils.ByteRange{From: 101}, ""},
		{"101:202", trustlessutils.ByteRange{From: 101, To: ptr(int64(202))}, ""},
		{"-101:-202", trustlessutils.ByteRange{From: -101, To: ptr(int64(-202))}, ""},
		{"0", trustlessutils.ByteRange{}, "invalid byte range: \"0\""},
		{"bork", trustlessutils.ByteRange{}, "invalid byte range: \"bork\""},
		{"0:x", trustlessutils.ByteRange{}, "invalid byte range: \"0:x\" (\"x\" is not an integer)"},
		{"y:*", trustlessutils.ByteRange{}, "invalid byte range: \"y:*\" (\"y\" is not an integer)"},
		{"101-202", trustlessutils.ByteRange{}, "invalid byte range: \"101-202\""},
		{"101:202:303", trustlessutils.ByteRange{}, "invalid byte range: \"101:202:303\""},
		{"101:202:*", trustlessutils.ByteRange{}, "invalid byte range: \"101:202:*\""},
	} {
		t.Run(tc.input, func(t *testing.T) {
			br, err := trustlessutils.ParseByteRange(tc.input)
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

func TestRequestSelector(t *testing.T) {
	// explore interpret-as (~), next (>), union (|) of match (.) and explore recursive (R) edge (@) with a depth of 1, interpreted as unixfs
	matchUnixfsEntityJson := `{"~":{">":{"|":[{".":{}},{"R":{":>":{"a":{">":{"@":{}}}},"l":{"depth":1}}}]},"as":"unixfs"}}`
	// explore interpret-as (~), next (>), union (|) of match subset with range, and explore recursive (R) edge (@) with a depth of 1, interpreted as unixfs
	matchUnixfsEntitySliceJsonFmt := `{"~":{">":{"|":[{".":{"subset":{"[":%d,"]":%d}}},{"R":{":>":{"a":{">":{"@":{}}}},"l":{"depth":1}}}]},"as":"unixfs"}}`
	exploreAll := `{"R":{":>":{"a":{">":{"@":{}}}},"l":{"none":{}}}}` // CommonSelector_ExploreAllRecursively
	matchPoint := `{".":{}}`

	jsonFields := func(target string, fields ...string) string {
		var sb strings.Builder
		for _, n := range fields {
			// explore interpret-as (~) next (>), explore field (f) + specific field (f>), with field name
			sb.WriteString(fmt.Sprintf(`{"~":{">":{"f":{"f>":{"%s":`, n))
		}
		sb.WriteString(target)
		sb.WriteString(strings.Repeat(`}}},"as":"unixfs"}}`, len(fields)))
		return sb.String()
	}

	for _, tc := range []struct {
		name string
		req  trustlessutils.Request
		sel  string
	}{
		{
			name: "empty",
			req:  trustlessutils.Request{},
			sel:  exploreAll,
		},
		{
			name: "all",
			req:  trustlessutils.Request{Scope: trustlessutils.DagScopeAll},
			sel:  exploreAll,
		},
		{
			name: "entity",
			req:  trustlessutils.Request{Scope: trustlessutils.DagScopeEntity},
			sel:  matchUnixfsEntityJson,
		},
		{
			name: "block",
			req:  trustlessutils.Request{Scope: trustlessutils.DagScopeBlock},
			sel:  matchPoint,
		},
		{
			name: "path + empty",
			req:  trustlessutils.Request{Path: "foo/bar/baz"},
			sel:  jsonFields(exploreAll, "foo", "bar", "baz"),
		},
		{
			name: "path + all",
			req:  trustlessutils.Request{Path: "foo/bar/baz", Scope: trustlessutils.DagScopeAll},
			sel:  jsonFields(exploreAll, "foo", "bar", "baz"),
		},
		{
			name: "path + entity",
			req:  trustlessutils.Request{Path: "foo/bar/baz", Scope: trustlessutils.DagScopeEntity},
			sel:  jsonFields(matchUnixfsEntityJson, "foo", "bar", "baz"),
		},
		{
			name: "path + block",
			req:  trustlessutils.Request{Path: "foo/bar/baz", Scope: trustlessutils.DagScopeBlock},
			sel:  jsonFields(matchPoint, "foo", "bar", "baz"),
		},
		{
			name: "byte range entity",
			req:  trustlessutils.Request{Scope: trustlessutils.DagScopeEntity, Bytes: &trustlessutils.ByteRange{From: 100, To: ptr(200)}},
			sel:  fmt.Sprintf(matchUnixfsEntitySliceJsonFmt, 100, 201), // note 200->201, inclusive->exclusive
		},
		{
			name: "byte range all",
			req:  trustlessutils.Request{Scope: trustlessutils.DagScopeAll, Bytes: &trustlessutils.ByteRange{From: 100, To: ptr(200)}},
			sel:  exploreAll,
		},
		{
			name: "byte range block",
			req:  trustlessutils.Request{Scope: trustlessutils.DagScopeBlock, Bytes: &trustlessutils.ByteRange{From: 100, To: ptr(200)}},
			sel:  matchPoint,
		},
		{
			name: "path + byte range entity",
			req:  trustlessutils.Request{Path: "foo/bar/baz", Scope: trustlessutils.DagScopeEntity, Bytes: &trustlessutils.ByteRange{From: -100, To: ptr(-200)}},
			sel:  jsonFields(fmt.Sprintf(matchUnixfsEntitySliceJsonFmt, -100, -200), "foo", "bar", "baz"), // note 200 not transformed for negative
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selNode := tc.req.Selector()
			selStr, err := ipld.Encode(selNode, dagjson.Encode)
			require.NoError(t, err)
			require.Equal(t, tc.sel, string(selStr))
		})
	}
}

func TestEtag(t *testing.T) {
	// To generate independent fixtures using Node.js, `npm install xxhash` then
	// in a REPL:
	//
	//   xx = (s) => require('xxhash').hash64(Buffer.from(s), 0).readBigUInt64LE(0).toString(32)
	//
	// then generate the suffix with the expected construction:
	//
	//   xx('/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.dfs')

	testCases := []struct {
		cid      cid.Cid
		path     string
		scope    trustlessutils.DagScope
		bytes    *trustlessutils.ByteRange
		dups     bool
		expected string
	}{
		{
			cid:      testCidV0,
			scope:    trustlessutils.DagScopeAll,
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.8it8cu7ifb381"`,
		},
		{
			cid:      testCidV0,
			scope:    trustlessutils.DagScopeEntity,
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.foi1g1a8rg6ti"`,
		},
		{
			cid:      testCidV0,
			scope:    trustlessutils.DagScopeBlock,
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.a8f7a8rsrms9i"`,
		},
		{
			cid:      testCidV0,
			scope:    trustlessutils.DagScopeAll,
			dups:     true,
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.6m8kn4j19tni6"`,
		},
		{
			cid:      testCidV0,
			scope:    trustlessutils.DagScopeEntity,
			dups:     true,
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.2j3kq27gtjhf"`,
		},
		{
			cid:      testCidV0,
			scope:    trustlessutils.DagScopeBlock,
			dups:     true,
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.2ri95n4cje0rr"`,
		},
		{
			cid:      testCidV1,
			scope:    trustlessutils.DagScopeAll,
			path:     "/some/path/to/thing",
			expected: `W/"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.dsohuj12hih8i"`,
		},
		{
			cid:      testCidV1,
			scope:    trustlessutils.DagScopeEntity,
			path:     "/some/path/to/thing",
			expected: `W/"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.aq30ilfrin04i"`,
		},
		{
			cid:      testCidV1,
			scope:    trustlessutils.DagScopeBlock,
			path:     "/some/path/to/thing",
			expected: `W/"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.cs4m0tf4dl7ju"`,
		},
		{
			cid:      testCidV1,
			scope:    trustlessutils.DagScopeAll,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `W/"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.dtit638f59s4p"`,
		},
		{
			cid:      testCidV1,
			scope:    trustlessutils.DagScopeEntity,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `W/"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.e8bn343seg1kp"`,
		},
		{
			cid:      testCidV1,
			scope:    trustlessutils.DagScopeBlock,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `W/"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.1oe35hpqdh19"`,
		},
		// path variations should be normalised
		{
			cid:      testCidV1,
			scope:    trustlessutils.DagScopeAll,
			path:     "some/path/to/thing",
			expected: `W/"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.dsohuj12hih8i"`,
		},
		{
			cid:      testCidV1,
			scope:    trustlessutils.DagScopeAll,
			path:     "///some//path//to/thing/",
			expected: `W/"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.dsohuj12hih8i"`,
		},
		{
			cid:      cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk"),
			scope:    trustlessutils.DagScopeAll,
			expected: `W/"bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk.car.dpv90ba8ck8dn"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    trustlessutils.DagScopeAll,
			bytes:    &trustlessutils.ByteRange{From: 0}, // default, not included
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.8it8cu7ifb381"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    trustlessutils.DagScopeAll,
			bytes:    &trustlessutils.ByteRange{From: 10},
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.e5uv1fivtc00q"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    trustlessutils.DagScopeAll,
			bytes:    &trustlessutils.ByteRange{From: 0, To: ptr(200)},
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.d85s2ubukqum"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    trustlessutils.DagScopeAll,
			bytes:    &trustlessutils.ByteRange{From: 100, To: ptr(200)},
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.5rhbacaeam153"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    trustlessutils.DagScopeEntity,
			bytes:    &trustlessutils.ByteRange{From: 100, To: ptr(200)},
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.qlvlk4h7odk6"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    trustlessutils.DagScopeEntity,
			dups:     true,
			bytes:    &trustlessutils.ByteRange{From: 100, To: ptr(200)},
			expected: `W/"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.272msbj2cl4lj"`,
		},
	}

	for _, tc := range testCases {
		br := ""
		if tc.bytes != nil {
			br = ":" + tc.bytes.String()
		}
		t.Run(fmt.Sprintf("%s:%s:%s:%v%s", tc.cid.String(), tc.path, tc.scope, tc.dups, br), func(t *testing.T) {
			rr := trustlessutils.Request{
				Root:       tc.cid,
				Path:       tc.path,
				Scope:      tc.scope,
				Bytes:      tc.bytes,
				Duplicates: tc.dups,
			}
			actual := rr.Etag("dfs") // Default order
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

func TestUrlPath(t *testing.T) {
	testCases := []struct {
		name            string
		request         trustlessutils.Request
		expectedUrlPath string
	}{
		{
			name: "plain",
			request: trustlessutils.Request{
				Root: testCidV1,
			},
			expectedUrlPath: "?dag-scope=all",
		},
		{
			name: "path",
			request: trustlessutils.Request{
				Root: testCidV1,
				Path: "/some/path/to/thing",
			},
			expectedUrlPath: "/some/path/to/thing?dag-scope=all",
		},
		{
			name: "escaped path",
			request: trustlessutils.Request{
				Root: testCidV1,
				Path: "/?/#/;/&/ /!",
			},
			expectedUrlPath: "/%3F/%23/%3B/&/%20/%21?dag-scope=all",
		},
		{
			name: "entity",
			request: trustlessutils.Request{
				Root:  testCidV1,
				Scope: trustlessutils.DagScopeEntity,
			},
			expectedUrlPath: "?dag-scope=entity",
		},
		{
			name: "block",
			request: trustlessutils.Request{
				Root:  testCidV1,
				Scope: trustlessutils.DagScopeBlock,
			},
			expectedUrlPath: "?dag-scope=block",
		},
		{
			name: "duplicates",
			request: trustlessutils.Request{
				Root:       testCidV0,
				Duplicates: true,
			},
			expectedUrlPath: "?dag-scope=all",
		},
		{
			name: "byte range",
			request: trustlessutils.Request{
				Root:  testCidV1,
				Bytes: &trustlessutils.ByteRange{From: 100, To: ptr(200)},
			},
			expectedUrlPath: "?dag-scope=all&entity-bytes=100:200",
		},
		{
			name: "byte range -ve",
			request: trustlessutils.Request{
				Root:  testCidV1,
				Bytes: &trustlessutils.ByteRange{From: -100},
			},
			expectedUrlPath: "?dag-scope=all&entity-bytes=-100:*",
		},
		{
			name: "all the things",
			request: trustlessutils.Request{
				Root:       testCidV0,
				Path:       "/some/path/to/thing",
				Scope:      trustlessutils.DagScopeEntity,
				Duplicates: true,
				Bytes:      &trustlessutils.ByteRange{From: 100, To: ptr(-200)},
			},
			expectedUrlPath: "/some/path/to/thing?dag-scope=entity&entity-bytes=100:-200",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := tc.request.UrlPath()
			require.NoError(t, err)
			require.Equal(t, tc.expectedUrlPath, actual)
		})
	}
}

func TestIpfsRoots(t *testing.T) {
	testCases := []struct {
		name     string
		request  trustlessutils.Request
		expected string
	}{
		{
			name: "simple CID request",
			request: trustlessutils.Request{
				Root: testCidV1,
			},
			expected: testCidV1.String(),
		},
		{
			name: "simple CID request with scope",
			request: trustlessutils.Request{
				Root:  testCidV0,
				Scope: trustlessutils.DagScopeEntity,
			},
			expected: testCidV0.String(),
		},
		{
			name: "CID with path - empty (streaming gateway)",
			request: trustlessutils.Request{
				Root: testCidV1,
				Path: "/some/path/to/thing",
			},
			expected: "",
		},
		{
			name: "CID with path and scope - empty (streaming gateway)",
			request: trustlessutils.Request{
				Root:  testCidV1,
				Path:  "some/path",
				Scope: trustlessutils.DagScopeEntity,
			},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.request.IpfsRoots()
			require.Equal(t, tc.expected, actual)
		})
	}
}

func ptr(i int64) *int64 {
	return &i
}
