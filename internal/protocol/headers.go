// Copyright (c) 2020 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

package protocol

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// ContentTypeDitto defines the Ditto JSON 'content-type' header value for Ditto Protocol messages.
	ContentTypeDitto = "application/vnd.eclipse.ditto+json"

	// ContentTypeJSON defines the JSON 'content-type' header value for Ditto Protocol messages.
	ContentTypeJSON = "application/json"

	// ContentTypeJSONMerge defines the JSON merge patch 'content-type' header value for Ditto Protocol messages,
	// as specified with RFC 7396 (https://datatracker.ietf.org/doc/html/rfc7396).
	ContentTypeJSONMerge = "application/merge-patch+json"

	headerContentType      = "content-type"
	headerCorrelationID    = "correlation-id"
	headerReplyTo          = "reply-to"
	headerResponseRequired = "response-required"
	headerTimeout          = "timeout"
	headerETag             = "etag"
	headerIfMatch          = "if-match"
	headerIfNoneMatch      = "if-none-match"
)

// Headers represents currently used Ditto headers along with additional HTTP headers
// that can be applied depending on the transport used.
// All protocol defined headers are case-insensitive and by default lowercase is used.
// See https://www.eclipse.org/ditto/protocol-specification.html
type Headers struct {
	values map[string]interface{}
}

// ContentType returns the 'content-type' header value or empty string if not set.
func (h *Headers) ContentType() string {
	if value, ok := h.values[headerContentType]; ok {
		return value.(string)
	}
	return ""
}

// WithContentType sets the 'content-type' header value if non-empty contentType is provided,
// otherwise removes the 'content-type' header.
func (h *Headers) WithContentType(contentType string) *Headers {
	if len(contentType) > 0 {
		h.values[headerContentType] = contentType
	} else {
		delete(h.values, headerContentType)
	}
	return h
}

// CorrelationID returns the 'correlation-id' header value or empty string if not set.
func (h *Headers) CorrelationID() string {
	if value, ok := h.values[headerCorrelationID]; ok {
		return value.(string)
	}
	return ""
}

// WithCorrelationID sets the 'correlation-id' header value if non-empty correlationID is provided,
// otherwise removes the 'correlation-id' header.
func (h *Headers) WithCorrelationID(correlationID string) *Headers {
	if len(correlationID) > 0 {
		h.values[headerCorrelationID] = correlationID
	} else {
		delete(h.values, headerCorrelationID)
	}
	return h
}

// ReplyTo returns the 'reply-to' header value or empty string if not set.
func (h *Headers) ReplyTo() string {
	if value, ok := h.values[headerReplyTo]; ok {
		return value.(string)
	}
	return ""
}

// WithReplyTo sets the 'reply-to' header value if non-empty replyTo is provided,
// otherwise removes the 'reply-to' header.
func (h *Headers) WithReplyTo(replyTo string) *Headers {
	if len(replyTo) > 0 {
		h.values[headerReplyTo] = replyTo
	} else {
		delete(h.values, headerReplyTo)
	}
	return h
}

// Timeout returns the 'timeout' header value or duration of 60 seconds if not set.
func (h *Headers) Timeout() time.Duration {
	if value, ok := h.values[headerTimeout]; ok {
		if duration, err := parseTimeout(value.(string)); err == nil {
			return duration
		}
	}
	return 60 * time.Second
}

func parseTimeout(timeout string) (time.Duration, error) {
	l := len(timeout)
	if l > 0 {
		t := time.Duration(-1)
		switch timeout[l-1] {
		case 'm':
			if i, err := strconv.Atoi(timeout[:l-1]); err == nil {
				t = time.Duration(i) * time.Minute
			}
		case 's':
			if timeout[l-2] == 'm' {
				if i, err := strconv.Atoi(timeout[:l-2]); err == nil {
					t = time.Duration(i) * time.Millisecond
				}
			} else {
				if i, err := strconv.Atoi(timeout[:l-1]); err == nil {
					t = time.Duration(i) * time.Second
				}
			}
		default:
			if i, err := strconv.Atoi(timeout); err == nil {
				t = time.Duration(i) * time.Second
			}
		}
		if inTimeoutRange(t) {
			return t, nil
		}
	}
	return -1, fmt.Errorf("invalid timeout '%s'", timeout)
}

// WithTimeout sets the 'timeout' header value.
//
// The provided value should be a non-negative duration in Millisecond, Second or Minute unit.
// The change results in timeout header string value containing the duration
// and the unit symbol (ms, s or m), e.g. '45s' or '250ms' or '1m'.
//
// The default value is '60s'.
// If a negative duration or duration of an hour or more is provided, the timeout header value
// is removed, i.e. the default one is used.
func (h *Headers) WithTimeout(timeout time.Duration) *Headers {
	if inTimeoutRange(timeout) {
		var value string

		if timeout > time.Second {
			div := int64(timeout / time.Second)
			rem := timeout % time.Second
			if rem == 0 {
				value = strconv.FormatInt(div, 10)
			} else {
				value = strconv.FormatInt(div+1, 10)
			}
		} else {
			div := int64(timeout / time.Millisecond)
			rem := timeout % time.Millisecond
			if rem == 0 {
				value = strconv.FormatInt(div, 10) + "ms"
			} else {
				value = strconv.FormatInt(div+1, 10) + "ms"
			}
		}

		h.values[headerTimeout] = value
	} else {
		delete(h.values, headerTimeout)
	}
	return h
}

func inTimeoutRange(timeout time.Duration) bool {
	return timeout >= 0 && timeout < time.Hour
}

// ResponseRequired returns 'true' if the 'response-required' header is set,
// otherwise 'false'.
func (h *Headers) ResponseRequired() bool {
	if value, ok := h.values[headerResponseRequired]; ok {
		return value.(bool)
	}
	return true
}

// WithResponseRequired sets the 'response-required' header value if 'true' value is provided,
// otherwise removes the 'response-required' header.
func (h *Headers) WithResponseRequired(isResponseRequired bool) *Headers {
	if isResponseRequired {
		delete(h.values, headerResponseRequired)
	} else {
		h.values[headerResponseRequired] = isResponseRequired
	}
	return h
}

// ETag returns the 'etag' header value or empty string if not set.
func (h *Headers) ETag() string {
	if value, ok := h.values[headerETag]; ok {
		return value.(string)
	}
	return ""
}

// WithETag sets the 'etag' header value if non-empty etag is provided,
// otherwise removes the 'etag' header.
func (h *Headers) WithETag(eTag string) *Headers {
	if len(eTag) > 0 {
		h.values[headerETag] = eTag
	} else {
		delete(h.values, headerETag)
	}
	return h
}

// IfMatch returns the 'if-match' header value or empty string if not set.
func (h *Headers) IfMatch() string {
	if value, ok := h.values[headerIfMatch]; ok {
		return value.(string)
	}
	return ""
}

// WithIfMatch sets the 'if-match' header value if non-empty ifMatch is provided,
// otherwise removes the 'if-match' header.
func (h *Headers) WithIfMatch(ifMatch string) *Headers {
	if len(ifMatch) > 0 {
		h.values[headerIfMatch] = ifMatch
	} else {
		delete(h.values, headerIfMatch)
	}
	return h
}

// IfNoneMatch returns the 'if-none-match' header value or empty string if not set.
func (h *Headers) IfNoneMatch() string {
	if value, ok := h.values[headerIfNoneMatch]; ok {
		return value.(string)
	}
	return ""
}

// WithIfNoneMatch sets the 'if-none-match' header value if non-empty ifNoneMatch is provided,
// otherwise removes the 'if-none-match' header.
func (h *Headers) WithIfNoneMatch(ifNoneMatch string) *Headers {
	if len(ifNoneMatch) > 0 {
		h.values[headerIfNoneMatch] = ifNoneMatch
	} else {
		delete(h.values, headerIfNoneMatch)
	}
	return h
}

// Generic returns the value of the provided key header and if a header with such key is present.
func (h *Headers) Generic(key string) (interface{}, bool) {
	v, ok := h.values[strings.ToLower(key)]
	return v, ok
}

// WithGeneric sets the value of the provided key header.
// If nil or empty string is provided, the header with the provided key is removed.
func (h *Headers) WithGeneric(key string, value interface{}) *Headers {
	if s, ok := value.(string); ok {
		name := strings.ToLower(key)
		if len(s) > 0 {
			h.values[name] = s
		} else {
			delete(h.values, name)
		}
	} else {
		h.values[strings.ToLower(key)] = value
	}
	return h
}

// NewHeaders creates an instance with no headers set.
func NewHeaders() *Headers {
	return &Headers{
		values: make(map[string]interface{}),
	}
}

// Clone creates a new instance with all headers copied.
func (h *Headers) Clone() *Headers {
	headers := NewHeaders()
	for key, value := range h.values {
		headers.values[key] = value
	}
	return headers
}

// MarshalJSON returns the JSON encoding of all headers.
func (h *Headers) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.values)
}

// UnmarshalJSON parses the JSON-encoded data and initializes the headers with the result.
// Error is returned if there is unexpected data format or if the data contains an invalid
// string representation of timeout header value.
//
// The timeout should be a non-negative value and unit symbol (ms, s or m), e.g. '45s' or '250ms' or '1m'.
// If the unit symbol is not provided, the value is interpreted as provided in seconds.
func (h *Headers) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	for k, v := range m {
		for i := 0; i < len(k); i++ {
			c := k[i]
			if 'A' <= c && c <= 'Z' {
				delete(m, k)
				m[strings.ToLower(k)] = v
			}
		}
	}

	if value, ok := m[headerTimeout]; ok {
		if _, err := parseTimeout(value.(string)); err != nil {
			return err
		}
	}

	h.values = m

	return nil
}
