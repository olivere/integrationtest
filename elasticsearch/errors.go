package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// ParseError will parse the response and return a decoded Error returned
// by Elasticsearch.
//
// 1. If err is not nil, return err.
// 2. If res is nil, return nil.
// 3. If res is not an error, return nil.
// 4. If res cannot be decoded into an ES error, return the decoding error.
// 4. Return an *Error instance.
func ParseError(res *esapi.Response, err error) error {
	if err != nil {
		return err
	}
	if res == nil || !res.IsError() {
		return nil
	}
	var e Error
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return err
	}
	return &e
}

// ParseHTTPResponse is like ParseError but expects a raw http.Response.
func ParseHTTPResponse(res *http.Response, err error) error {
	if err != nil {
		return err
	}
	if res == nil || res.StatusCode < 300 {
		return nil
	}
	var e Error
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return &e
	}
	return err
}

// Error encapsulates error details as returned from Elasticsearch.
type Error struct {
	Status  int           `json:"status"`
	Details *ErrorDetails `json:"error,omitempty"`
}

// ErrorDetails encapsulate error details from Elasticsearch.
// It is used in e.g. elastic.Error and elastic.BulkResponseItem.
type ErrorDetails struct {
	Type         string                   `json:"type"`
	Reason       string                   `json:"reason"`
	ResourceType string                   `json:"resource.type,omitempty"`
	ResourceId   string                   `json:"resource.id,omitempty"`
	Index        string                   `json:"index,omitempty"`
	Phase        string                   `json:"phase,omitempty"`
	Grouped      bool                     `json:"grouped,omitempty"`
	CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
	RootCause    []*ErrorDetails          `json:"root_cause,omitempty"`
	FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`

	// ScriptException adds the information in the following block.

	ScriptStack []string             `json:"script_stack,omitempty"` // from ScriptException
	Script      string               `json:"script,omitempty"`       // from ScriptException
	Lang        string               `json:"lang,omitempty"`         // from ScriptException
	Position    *ScriptErrorPosition `json:"position,omitempty"`     // from ScriptException (7.7+)
}

// ScriptErrorPosition specifies the position of the error
// in a script. It is used in ErrorDetails for scripting errors.
type ScriptErrorPosition struct {
	Offset int `json:"offset"`
	Start  int `json:"start"`
	End    int `json:"end"`
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	if e.Details != nil && e.Details.Reason != "" {
		return fmt.Sprintf("elasticsearch: Error %d (%s): %s [type=%s]", e.Status, http.StatusText(e.Status), e.Details.Reason, e.Details.Type)
	}
	return fmt.Sprintf("elasticsearch: Error %d (%s)", e.Status, http.StatusText(e.Status))
}

// ErrorReason returns the reason of an error that Elasticsearch reported,
// if err is of kind Error and has ErrorDetails with a Reason. Any other
// value of err will return an empty string.
func ErrorReason(err error) string {
	if err == nil {
		return ""
	}
	e, ok := err.(*Error)
	if !ok || e == nil || e.Details == nil {
		return ""
	}
	return e.Details.Reason
}

// IsContextErr returns true if the error is from a context that was canceled or deadline exceeded
func IsContextErr(err error) bool {
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	// This happens e.g. on redirect errors, see https://golang.org/src/net/http/client_test.go#L329
	if ue, ok := err.(*url.Error); ok {
		if ue.Temporary() {
			return true
		}
		// Use of an AWS Signing Transport can result in a wrapped url.Error
		return IsContextErr(ue.Err)
	}
	return false
}

// IsNotFound returns true if the given error indicates that Elasticsearch
// returned HTTP status 404. The err parameter can be of type *elastic.Error,
// elastic.Error, *http.Response or int (indicating the HTTP status code).
func IsNotFound(err interface{}) bool {
	return IsStatusCode(err, http.StatusNotFound)
}

// IsTimeout returns true if the given error indicates that Elasticsearch
// returned HTTP status 408. The err parameter can be of type *elastic.Error,
// elastic.Error, *http.Response or int (indicating the HTTP status code).
func IsTimeout(err interface{}) bool {
	return IsStatusCode(err, http.StatusRequestTimeout)
}

// IsConflict returns true if the given error indicates that the Elasticsearch
// operation resulted in a version conflict. This can occur in operations like
// `update` or `index` with `op_type=create`. The err parameter can be of
// type *elastic.Error, elastic.Error, *http.Response or int (indicating the
// HTTP status code).
func IsConflict(err interface{}) bool {
	return IsStatusCode(err, http.StatusConflict)
}

// IsUnauthorized returns true if the given error indicates that
// Elasticsearch returned HTTP status 401. This happens e.g. when the
// cluster is configured to require HTTP Basic Auth.
// The err parameter can be of type *elastic.Error, elastic.Error,
// *http.Response or int (indicating the HTTP status code).
func IsUnauthorized(err interface{}) bool {
	return IsStatusCode(err, http.StatusUnauthorized)
}

// IsForbidden returns true if the given error indicates that Elasticsearch
// returned HTTP status 403. This happens e.g. due to a missing license.
// The err parameter can be of type *elastic.Error, elastic.Error,
// *http.Response or int (indicating the HTTP status code).
func IsForbidden(err interface{}) bool {
	return IsStatusCode(err, http.StatusForbidden)
}

// IsStatusCode returns true if the given error indicates that the Elasticsearch
// operation returned the specified HTTP status code. The err parameter can be of
// type *http.Response, *Error, Error, or int (indicating the HTTP status code).
func IsStatusCode(err interface{}, code int) bool {
	switch e := err.(type) {
	case *esapi.Response:
		return e.StatusCode == code
	case *http.Response:
		return e.StatusCode == code
	case *Error:
		return e.Status == code
	case Error:
		return e.Status == code
	case int:
		return e == code
	}
	return false
}
