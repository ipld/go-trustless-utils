package trustlesshttp

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	trustlessutils "github.com/ipld/go-trustless-utils"
)

// ParseScope returns the dag-scope query parameter or an error if the dag-scope
// parameter is not one of the supported values.
func ParseScope(req *http.Request) (trustlessutils.DagScope, error) {
	if req.URL.Query().Has("dag-scope") {
		if ds, err := trustlessutils.ParseDagScope(req.URL.Query().Get("dag-scope")); err != nil {
			return ds, errors.New("invalid dag-scope parameter")
		} else {
			return ds, nil
		}
	}
	return trustlessutils.DagScopeAll, nil
}

// ParseByteRange returns the entity-bytes query parameter if one is set in the
// query string or nil if one is not set. An error is returned if an
// entity-bytes query string is not a valid byte range.
func ParseByteRange(req *http.Request) (*trustlessutils.ByteRange, error) {
	if req.URL.Query().Has("entity-bytes") {
		br, err := trustlessutils.ParseByteRange(req.URL.Query().Get("entity-bytes"))
		if err != nil {
			return nil, errors.New("invalid entity-bytes parameter")
		}
		return &br, nil
	}
	return nil, nil
}

// ParseFilename returns the filename query parameter or an error if the
// filename extension is not ".car". Lassie only supports returning CAR data.
// See https://specs.ipfs.tech/http-gateways/path-gateway/#filename-request-query-parameter
func ParseFilename(req *http.Request) (string, error) {
	// check if provided filename query parameter has .car extension
	if req.URL.Query().Has("filename") {
		filename := req.URL.Query().Get("filename")
		ext := filepath.Ext(filename)
		if ext == "" {
			return "", errors.New("invalid filename parameter; missing extension")
		}
		if ext != FilenameExtCar {
			return "", fmt.Errorf("invalid filename parameter; unsupported extension: %q", ext)
		}
		return filename, nil
	}
	return "", nil
}

// CheckFormat validates that the data being requested is of the type CAR.
// We do this validation because the IPFS Path Gateway spec allows for
// additional response formats that the IPFS Trustless Gateway spec does not
// currently support, so we throw an error in the cases where the request is
// requesting one the unsupported response formats. IPFS Trustless Gateway only
// supports returning CAR data.
//
// The spec outlines that the requesting format can be provided
// via the Accept header or the format query parameter.
//
// IPFS Trustless Gateway only allows the application/vnd.ipld.car Accept header
// https://specs.ipfs.tech/http-gateways/path-gateway/#accept-request-header
//
// IPFS Trustless Gateway only allows the "car" format query parameter
// https://specs.ipfs.tech/http-gateways/path-gateway/#format-request-query-parameter
func CheckFormat(req *http.Request) (bool, error) {
	includeDupes := DefaultIncludeDupes

	accept := req.Header.Get("Accept")
	if accept != "" {
		// check if Accept header includes application/vnd.ipld.car
		var validAccept bool
		validAccept, includeDupes = ParseAccept(accept)
		if !validAccept {
			return false, fmt.Errorf("invalid Accept header; unsupported: %q", accept)
		}
	}
	// check if format is "car"
	format := req.URL.Query().Get("format")
	if format != "" && format != FormatParameterCar {
		return false, fmt.Errorf("invalid format parameter; unsupported: %q", format)
	}

	// if neither are provided return
	// one of them has to be given with a CAR type since we only return CAR data
	if accept == "" && format == "" {
		return false, fmt.Errorf("neither a valid Accept header nor format parameter were provided")
	}

	return includeDupes, nil
}

// ParseAccept validates a request Accept header and returns whether or not
// duplicate blocks are allowed in the response.
//
// This will operate the same as ParseContentType except that it is less strict
// with the format specifier, allowing for "application/*" and "*/*" as well as
// the standard "application/vnd.ipld.car".
func ParseAccept(acceptHeader string) (validAccept bool, includeDupes bool) {
	return parseContentType(acceptHeader, false)
}

// ParseContentType validates a response Content-Type header and returns whether
// or not duplicate blocks are expected in the response.
//
// This will operate the same as ParseAccept except that it strictly only
// allows the "application/vnd.ipld.car" Content-Type.
func ParseContentType(contentTypeHeader string) (validContentType bool, includeDupes bool) {
	return parseContentType(contentTypeHeader, true)
}

func parseContentType(header string, strictType bool) (validAccept bool, includeDupes bool) {
	acceptTypes := strings.Split(header, ",")
	validAccept = false
	includeDupes = DefaultIncludeDupes
	for _, acceptType := range acceptTypes {
		typeParts := strings.Split(acceptType, ";")
		if typeParts[0] == MimeTypeCar || (!strictType && (typeParts[0] == "*/*" || typeParts[0] == "application/*")) {
			validAccept = true
			if typeParts[0] == MimeTypeCar {
				// parse additional car attributes outlined in IPIP-412: https://github.com/ipfs/specs/pull/412
				for _, nextPart := range typeParts[1:] {
					pair := strings.Split(nextPart, "=")
					if len(pair) == 2 {
						attr := strings.TrimSpace(pair[0])
						value := strings.TrimSpace(pair[1])
						switch attr {
						case "dups":
							switch value {
							case "y":
								includeDupes = true
							case "n":
								includeDupes = false
							default:
								// don't accept unexpected values
								validAccept = false
							}
						case "version":
							switch value {
							case MimeTypeCarVersion:
							default:
								validAccept = false
							}
						case "order":
							switch value {
							case "dfs":
							case "unk":
							default:
								// we only do dfs, which also satisfies unk, future extensions are not yet supported
								validAccept = false
							}
						default:
							// ignore others
						}
					}
				}
			}
			// only break if further validation didn't fail
			if validAccept {
				break
			}
		}
	}
	return
}

var (
	ErrPathNotFound = errors.New("not found")
	ErrBadCid       = errors.New("failed to parse root CID")
)

// ParseUrlPath parses an incoming IPFS Trustless Gateway path of the form
// /ipfs/<cid>[/<path>] and returns the root CID and the path.
func ParseUrlPath(urlPath string) (cid.Cid, datamodel.Path, error) {
	path := datamodel.ParsePath(urlPath)
	var seg datamodel.PathSegment
	seg, path = path.Shift()
	if seg.String() != "ipfs" {
		return cid.Undef, datamodel.Path{}, ErrPathNotFound
	}

	// check if CID path param is missing
	if path.Len() == 0 {
		// not a valid path to hit
		return cid.Undef, datamodel.Path{}, ErrPathNotFound
	}

	// validate CID path parameter
	var cidSeg datamodel.PathSegment
	cidSeg, path = path.Shift()
	rootCid, err := cid.Parse(cidSeg.String())
	if err != nil {
		return cid.Undef, datamodel.Path{}, ErrBadCid
	}

	return rootCid, path, nil
}
