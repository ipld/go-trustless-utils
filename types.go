package trustlessutils

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

var matcherSelector = builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher()

// DagScope is used to represent the "dag-scope" parameter of the IPFS Trustless
// Gateway protocol.
type DagScope string

const (
	DagScopeAll    DagScope = "all"
	DagScopeEntity DagScope = "entity"
	DagScopeBlock  DagScope = "block"
)

// ParseDagScope parses a string form of a DagScope into a DagScope.
func ParseDagScope(s string) (DagScope, error) {
	switch s {
	case "all":
		return DagScopeAll, nil
	case "entity":
		return DagScopeEntity, nil
	case "block":
		return DagScopeBlock, nil
	default:
		return DagScopeAll, fmt.Errorf("invalid DagScope: %q", s)
	}
}

// TerminalSelectorSpec returns the IPLD selector spec that should be used for
// the terminal of the given DagScope.
func (ds DagScope) TerminalSelectorSpec() builder.SelectorSpec {
	switch ds {
	case DagScopeAll:
		return unixfsnode.ExploreAllRecursivelySelector
	case DagScopeEntity:
		return unixfsnode.MatchUnixFSEntitySelector
	case DagScopeBlock:
		return matcherSelector
	}
	return unixfsnode.ExploreAllRecursivelySelector // default to explore-all for zero-value and unknown DagScope
}

// ByteRange is used to represent the "entity-bytes" parameter of the IPFS
// Trustless Gateway protocol.
type ByteRange struct {
	From int64
	To   *int64 // To is a pointer to represent "*" as nil
}

// IsDefault is roughly equivalent to the range matching [0:*]
func (br *ByteRange) IsDefault() bool {
	return br == nil || br.From == 0 && br.To == nil
}

// String will produce a string form of the ByteRange suitable for use in a URL
// and parsable by ParseByteRange.
func (br *ByteRange) String() string {
	if br.IsDefault() {
		return "0:*"
	}
	to := "*" // default to end of file
	if br.To != nil {
		to = strconv.FormatInt(*br.To, 10)
	}
	return fmt.Sprintf("%d:%s", br.From, to)
}

// ParseByteRange parses a string form of a ByteRange into a ByteRange. It can
// be used to parse an "entity-bytes" parameter from a URL.
func ParseByteRange(s string) (ByteRange, error) {
	br := ByteRange{}
	if s == "" {
		return br, nil
	}
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return br, fmt.Errorf("invalid byte range: %q", s)
	}
	var err error
	br.From, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return br, fmt.Errorf("invalid byte range: %q (%q is not an integer)", s, parts[0])
	}
	if parts[1] != "*" {
		to, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return br, fmt.Errorf("invalid byte range: %q (%q is not an integer)", s, parts[1])
		}
		br.To = &to
	}
	return br, nil
}

// Request describes the parameters of an IPFS Trustless Gateway request.
// It is intended to be immutable.
type Request struct {
	// Root is the root CID to fetch.
	Root cid.Cid

	// Path is the optional path within the DAG to fetch.
	Path string

	// Scope describes the scope of the DAG to fetch. If the Selector parameter
	// is not set, Scope and Path will be used to construct a selector.
	Scope DagScope

	// Bytes is the optional byte range within the DAG to fetch. If not set
	// the default byte range will fetch the entire file.
	Bytes *ByteRange

	// Duplicates is a flag that indicates whether duplicate blocks should be
	// stored into the LinkSystem where they occur in the traversal.
	Duplicates bool
}

// Selector generates an IPLD selector for this Request.
//
// Note that only Path, Scope and Bytes are used to generate a selector; so
// a construction such as the following may be used to easily generate a
// Trustless Gateway, UnixFS compatible selector:
//
//	Request{Path: path, Scope: scope, Bytes: byteRange}.Selector()
func (r Request) Selector() datamodel.Node {
	// Turn the path / scope into a selector
	terminal := r.Scope.TerminalSelectorSpec()
	// TODO: from the spec (https://specs.ipfs.tech/http-gateways/trustless-gateway/):
	//   > It implies dag-scope=entity
	// We may need to switch this to ignore the Scope if we have a non-default byte range.
	if r.Scope == DagScopeEntity && !r.Bytes.IsDefault() {
		var to int64 = math.MaxInt64
		if r.Bytes.To != nil {
			to = *r.Bytes.To
			if to >= 0 {
				to++ // selector is exclusive, so increment the end
			}
		}
		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		// If we reach a terminal and it's not a file, then we need to fall-back to the default
		// selector for the given scope. We do this with a union of the original terminal.
		// "entity" is a special case here which we can't just union with our matcher because it
		// has its own matcher in it which we need to replace with the subset matcher.
		terminal = ssb.ExploreInterpretAs("unixfs",
			ssb.ExploreUnion(
				ssb.MatcherSubset(r.Bytes.From, to),
				ssb.ExploreRecursive(
					selector.RecursionLimitDepth(1),
					ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
				),
			),
		)
	}
	return unixfsnode.UnixFSPathSelectorBuilder(r.Path, terminal, false)
}

// UrlPath returns a URL path and query string valid with the Trusted HTTP
// Gateway spec by combining the Path and the Scope of this request.
//
// The returned value includes a URL escaped form of the originally requested
// path.
func (r Request) UrlPath() (string, error) {
	scope := r.Scope
	if r.Scope == "" {
		scope = DagScopeAll
	}
	byteRange := ""
	if !r.Bytes.IsDefault() {
		byteRange = "&entity-bytes=" + r.Bytes.String()
	}
	path := PathEscape(r.Path)
	return fmt.Sprintf("%s?dag-scope=%s%s", path, scope, byteRange), nil
}

// PathEscape both cleans an IPLD path and URL escapes it so that it can be
// used in a URL path.
func PathEscape(path string) string {
	if path == "" {
		return path
	}
	var sb strings.Builder
	var ps datamodel.PathSegment
	p := datamodel.ParsePath(path)
	for p.Len() > 0 {
		ps, p = p.Shift()
		sb.WriteRune('/')
		sb.WriteString(url.PathEscape(ps.String()))
	}
	return sb.String()
}

// Etag produces a weak Etag suitable for use as an Etag HTTP response header.
// The order parameter should match the CAR order parameter from the ContentType.
//
// A weak Etag is used because:
//   - Different implementations may include different parameters in the hash
//   - Streaming gateways cannot include resolved path segments (only root+path)
//   - For non-static backends (such as Filecoin storage providers), DAG
//     availability may change over time as new deals are added
func (r Request) Etag(order string) string {
	h := xxhash.New()

	// Path (unresolved - differs from Boxo's resolved immutable path)
	h.Write([]byte("/ipfs/"))
	h.Write([]byte(r.Root.String()))
	if r.Path != "" {
		h.Write([]byte("/"))
		h.Write([]byte(datamodel.ParsePath(r.Path).String()))
	}

	// Scope: only include if not default (all)
	if r.Scope != DagScopeAll {
		h.Write([]byte("\x00scope="))
		h.Write([]byte(string(r.Scope)))
	}

	// Byte range: only include if not default
	if !r.Bytes.IsDefault() {
		h.Write([]byte("\x00range="))
		h.Write([]byte(strconv.FormatInt(r.Bytes.From, 10)))
		if r.Bytes.To != nil {
			h.Write([]byte(","))
			h.Write([]byte(strconv.FormatInt(*r.Bytes.To, 10)))
		}
	}

	// Order: only include if not default (dfs)
	if order != "" && order != "dfs" {
		h.Write([]byte("\x00order="))
		h.Write([]byte(order))
	}

	// Duplicates: only include if explicitly true (y)
	if r.Duplicates {
		h.Write([]byte("\x00dups=y"))
	}

	suffix := strconv.FormatUint(h.Sum64(), 32)
	return `W/"` + r.Root.String() + ".car." + suffix + `"`
}

// IpfsRoots returns the CID or CIDs that should be included in the X-Ipfs-Roots
// response header. For streaming-first gateways that don't pre-resolve paths,
// this returns just the root CID for simple requests (no path), and an empty
// string for path requests since intermediate CIDs are not known ahead of time.
//
// Implementations that are able to resolve the full path ahead of time may
// return a comma-separated list of all CIDs in the path and not use this
// method.
func (r Request) IpfsRoots() string {
	// For requests with paths, streaming gateways cannot provide intermediate CIDs
	// since headers are sent before path traversal completes
	if r.Path != "" {
		return ""
	}
	// For simple CID requests, return just the root
	return r.Root.String()
}
