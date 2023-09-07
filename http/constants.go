package trustlesshttp

import (
	"strconv"
	"strings"
)

type ContentTypeOrder string

const (
	MimeTypeCar                = "application/vnd.ipld.car"            // The only accepted MIME type
	MimeTypeCarVersion         = "1"                                   // We only accept version 1 of the MIME type
	FormatParameterCar         = "car"                                 // The only valid format parameter value
	FilenameExtCar             = ".car"                                // The only valid filename extension
	ResponseCacheControlHeader = "public, max-age=29030400, immutable" // Magic cache control values
	DefaultIncludeDupes        = true                                  // The default value for an unspecified "dups" parameter.
	DefaultOrder               = ContentTypeOrderDfs                   // The default value for an unspecified "order" parameter.

	ContentTypeOrderDfs ContentTypeOrder = "dfs"
	ContentTypeOrderUnk ContentTypeOrder = "unk"
)

var (
	ResponseChunkDelimeter = []byte("0\r\n") // An http/1.1 chunk delimeter, used for specifying an early end to the response
)

type ContentType struct {
	Mime       string
	Order      ContentTypeOrder
	Duplicates bool
	Quality    float32
}

func (ct ContentType) String() string {
	sb := strings.Builder{}
	sb.WriteString(ct.Mime)
	sb.WriteString(";version=")
	sb.WriteString(MimeTypeCarVersion)
	sb.WriteString(";order=")
	sb.WriteString(string(ct.Order))
	if ct.Duplicates {
		sb.WriteString(";dups=y")
	} else {
		sb.WriteString(";dups=n")
	}
	if ct.Quality < 1 && ct.Quality >= 0.00 {
		sb.WriteString(";q=")
		// write quality with max 3 decimal places
		sb.WriteString(strconv.FormatFloat(float64(ct.Quality), 'g', 3, 32))
	}
	return sb.String()
}

type ContentTypeOption func(ct *ContentType)

func WithContentTypeOrder(order ContentTypeOrder) ContentTypeOption {
	return func(ct *ContentType) {
		ct.Order = order
	}
}

func WithContentTypeDuplicates(duplicates bool) ContentTypeOption {
	return func(ct *ContentType) {
		ct.Duplicates = duplicates
	}
}

func WithContentTypeQuality(quality float32) ContentTypeOption {
	return func(ct *ContentType) {
		ct.Quality = quality
	}
}

func NewContentType(opt ...ContentTypeOption) ContentType {
	ct := ContentType{
		Mime:       MimeTypeCar,
		Order:      DefaultOrder,
		Duplicates: DefaultIncludeDupes,
		Quality:    1,
	}
	for _, o := range opt {
		o(&ct)
	}
	return ct
}

// ResponseContentTypeHeader returns the value for the Content-Type header for a
// Trustless Gateway response which will vary depending on whether duplicates
// are included or not. Otherwise, the header is the same for all responses.
//
// Deprecated: Use NewContentType().String() instead.
func ResponseContentTypeHeader(duplicates bool) string {
	ct := NewContentType()
	ct.Duplicates = duplicates
	return ct.String()
}

// RequestAcceptHeader returns the value for the Accept header for a Trustless
// Gateway request which will vary depending on whether duplicates are included
// or not. Otherwise, the header is the same for all requests.
//
// Deprecated: Use NewContentType().String() instead.
func RequestAcceptHeader(duplicates bool) string {
	return ResponseContentTypeHeader(duplicates)
}
