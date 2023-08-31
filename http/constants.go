package trustlesshttp

import "fmt"

const (
	MimeTypeCar                = "application/vnd.ipld.car"            // The only accepted MIME type
	MimeTypeCarVersion         = "1"                                   // We only accept version 1 of the MIME type
	FormatParameterCar         = "car"                                 // The only valid format parameter value
	FilenameExtCar             = ".car"                                // The only valid filename extension
	DefaultIncludeDupes        = true                                  // The default value for an unspecified "dups" parameter. See https://github.com/ipfs/specs/pull/412.
	ResponseAcceptRangesHeader = "bytes"                               // The only valid value for the Accept-Ranges header
	ResponseCacheControlHeader = "public, max-age=29030400, immutable" // Magic cache control values
)

var (
	ResponseChunkDelimeter = []byte("0\r\n") // An http/1.1 chunk delimeter, used for specifying an early end to the response
	baseContentType        = fmt.Sprintf("%s; version=%s; order=dfs", MimeTypeCar, MimeTypeCarVersion)
)

// ResponseContentTypeHeader returns the value for the Content-Type header for a
// Trustless Gateway response which will vary depending on whether duplicates
// are included or not. Otherwise, the header is the same for all responses.
func ResponseContentTypeHeader(duplicates bool) string {
	if duplicates {
		return baseContentType + "; dups=y"
	}
	return baseContentType + "; dups=n"
}

// RequestAcceptHeader returns the value for the Accept header for a Trustless
// Gateway request which will vary depending on whether duplicates are included
// or not. Otherwise, the header is the same for all requests.
func RequestAcceptHeader(duplicates bool) string {
	return ResponseContentTypeHeader(duplicates)
}
