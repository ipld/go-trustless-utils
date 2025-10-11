package trustlesshttp

import (
	"net/url"
	"strconv"
	"strings"
)

type ContentTypeOrder string

const (
	MimeTypeCar                = "application/vnd.ipld.car"            // One of two acceptable MIME types
	MimeTypeRaw                = "application/vnd.ipld.raw"            // One of two acceptable MIME types
	MimeTypeCarVersion         = "1"                                   // We only accept version 1 of the CAR MIME type
	FormatParameterCar         = "car"                                 // One of two acceptable format parameter values
	FormatParameterRaw         = "raw"                                 // One of two acceptable format parameter values
	FilenameExtCar             = ".car"                                // Valid filename extension for CAR responses
	FilenameExtRaw             = ".bin"                                // Valid filename extension for raw block responses
	ResponseCacheControlHeader = "public, max-age=29030400, immutable" // Magic cache control values
	DefaultIncludeDupes        = true                                  // The default value for an unspecified "dups" parameter.
	DefaultOrder               = ContentTypeOrderDfs                   // The default value for an unspecified "order" parameter.

	ContentTypeOrderDfs ContentTypeOrder = "dfs"
	ContentTypeOrderUnk ContentTypeOrder = "unk"
)

var (
	ResponseChunkDelimeter = []byte("0\r\n") // An http/1.1 chunk delimeter, used for specifying an early end to the response
)

// ContentType represents a Content-Type descriptor for use with the response
// Content-Type header or the request Accept header specifically for
// Trustless Gateway requests and responses.
type ContentType struct {
	MimeType   string
	Order      ContentTypeOrder
	Duplicates bool
	Quality    float32
}

func (ct ContentType) String() string {
	sb := strings.Builder{}
	sb.WriteString(ct.MimeType)
	if ct.MimeType == MimeTypeCar {
		sb.WriteString(";version=")
		sb.WriteString(MimeTypeCarVersion)
		sb.WriteString(";order=")
		sb.WriteString(string(ct.Order))
		if ct.Duplicates {
			sb.WriteString(";dups=y")
		} else {
			sb.WriteString(";dups=n")
		}
	}
	if ct.Quality < 1 && ct.Quality >= 0.00 {
		sb.WriteString(";q=")
		// write quality with max 3 decimal places
		sb.WriteString(strconv.FormatFloat(float64(ct.Quality), 'g', 3, 32))
	}
	return sb.String()
}

func (ct ContentType) IsRaw() bool {
	return ct.MimeType == MimeTypeRaw
}

func (ct ContentType) IsCar() bool {
	return ct.MimeType == MimeTypeCar || ct.MimeType == "application/*" || ct.MimeType == "*/*"
}

// WithOrder returns a new ContentType with the specified order.
func (ct ContentType) WithOrder(order ContentTypeOrder) ContentType {
	ct.Order = order
	return ct
}

// WithDuplicates returns a new ContentType with the specified duplicates.
func (ct ContentType) WithDuplicates(duplicates bool) ContentType {
	ct.Duplicates = duplicates
	return ct
}

// WithMime returns a new ContentType with the specified mime type.
func (ct ContentType) WithMimeType(mime string) ContentType {
	ct.MimeType = mime
	return ct
}

// WithQuality returns a new ContentType with the specified quality.
func (ct ContentType) WithQuality(quality float32) ContentType {
	ct.Quality = quality
	return ct
}

// ContentLocation returns the Content-Location header value for this ContentType
// if format negotiation occurred via Accept header but the URL lacks a format parameter.
// Returns empty string if format parameter is already present or if Content-Location
// should not be set.
//
// This helps HTTP caches store different formats separately.
func (ct ContentType) ContentLocation(requestURL string) string {
	// Parse the URL to check for format parameter
	u, err := url.Parse(requestURL)
	if err != nil {
		return ""
	}

	// If format parameter already present, don't set Content-Location
	if u.Query().Get("format") != "" {
		return ""
	}

	// Determine format parameter value
	formatParam := FormatParameterCar
	if ct.IsRaw() {
		formatParam = FormatParameterRaw
	}

	// Build Content-Location URL with format parameter
	contentLocation := u.Path
	if u.RawQuery != "" {
		contentLocation += "?" + u.RawQuery + "&format=" + formatParam
	} else {
		contentLocation += "?format=" + formatParam
	}

	return contentLocation
}

func DefaultContentType() ContentType {
	return ContentType{
		MimeType:   MimeTypeCar,
		Order:      DefaultOrder,
		Duplicates: DefaultIncludeDupes,
		Quality:    1,
	}
}

// ResponseContentTypeHeader returns the value for the Content-Type header for a
// Trustless Gateway response which will vary depending on whether duplicates
// are included or not. Otherwise, the header is the same for all responses.
//
// Deprecated: Use DefaultContentType().WithDuplicates(duplicates).String() instead.
func ResponseContentTypeHeader(duplicates bool) string {
	return DefaultContentType().WithDuplicates(duplicates).String()
}

// RequestAcceptHeader returns the value for the Accept header for a Trustless
// Gateway request which will vary depending on whether duplicates are included
// or not. Otherwise, the header is the same for all requests.
//
// Deprecated: Use DefaultContentType().WithDuplicates(duplicates).String() instead.
func RequestAcceptHeader(duplicates bool) string {
	return ResponseContentTypeHeader(duplicates)
}
