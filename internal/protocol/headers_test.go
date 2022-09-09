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

package protocol_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
)

func TestHeaders(t *testing.T) {
	h := `{
        "response-required": false,
        "correlation-id": "5a1c8922-4828-46f8-9be6-1cbfcfef0d52",
        "content-type": "application/vnd.eclipse.ditto+json",
        "reply-to":"command/t9138cc86fcd14181aa7b_hub",
        "etag": "hash:ba930ee8",
        "If-Match":"hash:ba930ee8",
        "If-None-Match":"hash:ba930ee8"
	}`

	var headers protocol.Headers
	require.NoError(t, json.Unmarshal([]byte(h), &headers))

	assert.False(t, headers.ResponseRequired())
	assert.Equal(t, "5a1c8922-4828-46f8-9be6-1cbfcfef0d52", headers.CorrelationID())
	assert.Equal(t, protocol.ContentTypeDitto, headers.ContentType())
	assert.Equal(t, "command/t9138cc86fcd14181aa7b_hub", headers.ReplyTo())
	assert.Equal(t, "hash:ba930ee8", headers.ETag())
	assert.Equal(t, "hash:ba930ee8", headers.IfMatch())
	assert.Equal(t, "hash:ba930ee8", headers.IfNoneMatch())
	assert.Equal(t, time.Second*60, headers.Timeout())
}

func TestHeadersBuild(t *testing.T) {
	headers := protocol.NewHeaders().
		WithContentType("application/merge-patch+json").
		WithCorrelationID("5a1c8922-4828-46f8-9be6-1cbfcfef0d52").
		WithReplyTo("command/t9138cc86fcd145181aa7b_hub").
		WithETag("hash:ba930ee8").
		WithIfMatch("hash:ba930ee8").
		WithIfNoneMatch("hash:ba930ee8").
		WithGeneric("Name", "value")

	assert.Equal(t, protocol.ContentTypeJSONMerge, headers.ContentType())
	assert.Equal(t, "5a1c8922-4828-46f8-9be6-1cbfcfef0d52", headers.CorrelationID())
	assert.Equal(t, "command/t9138cc86fcd145181aa7b_hub", headers.ReplyTo())
	assert.Equal(t, "hash:ba930ee8", headers.ETag())
	assert.Equal(t, "hash:ba930ee8", headers.IfMatch())
	assert.Equal(t, "hash:ba930ee8", headers.IfNoneMatch())

	v, ok := headers.Generic("name")
	assert.True(t, ok)
	assert.Equal(t, "value", v)
}

func TestHeadersClone(t *testing.T) {
	headers := protocol.NewHeaders().
		WithContentType("application/vnd.eclipse.ditto+json").
		WithCorrelationID("5a1c8922-4828-46f8-9be6-1cbfcfef0d52").
		WithReplyTo("command/t9138cc86fcd145181aa7b_hub").
		WithETag("hash:ba930ee8").
		WithIfMatch("hash:ba930ee8").
		WithIfNoneMatch("hash:ba930ee8").
		WithGeneric("Name", "value")

	headersClone := headers.Clone()
	assert.Equal(t, headers, headersClone)

	headersClone.WithGeneric("name", "newValue").
		WithResponseRequired(false)

	v, ok := headers.Generic("name")
	assert.True(t, ok)
	assert.Equal(t, "value", v)

	v, ok = headersClone.Generic("name")
	assert.True(t, ok)
	assert.Equal(t, "newValue", v)

	assert.True(t, headers.ResponseRequired())
	assert.False(t, headersClone.ResponseRequired())
}

func TestTimeout(t *testing.T) {
	h := `{
	    "timeout": "10s"
	}`
	var headers protocol.Headers
	require.NoError(t, json.Unmarshal([]byte(h), &headers))
	assert.Equal(t, time.Second*10, headers.Timeout())

	h = `{
	    "timeout": "500ms"
	}`
	require.NoError(t, json.Unmarshal([]byte(h), &headers))
	assert.Equal(t, time.Millisecond*500, headers.Timeout())

	h = `{
	    "timeout": "1m"
	}`
	require.NoError(t, json.Unmarshal([]byte(h), &headers))
	assert.Equal(t, time.Minute, headers.Timeout())

	h = `{
        "timeout": "10"
	}`
	require.NoError(t, json.Unmarshal([]byte(h), &headers))
	assert.Equal(t, time.Second*10, headers.Timeout())

	h = `{
        "timeout": "0"
	}`
	require.NoError(t, json.Unmarshal([]byte(h), &headers))
	assert.Equal(t, time.Second*0, headers.Timeout())
}

func TestTimeoutInvalid(t *testing.T) {
	timeoutTests := []string{
		`{ "timeout": "60m" }`,
		`{ "timeout": "3600" }`,
		`{ "timeout": "-5" }`,
		`{ "timeout": "" }`,
		`{ "timeout": "invalid" }`,
	}

	var headers protocol.Headers
	for _, h := range timeoutTests {
		assert.Error(t, json.Unmarshal([]byte(h), &headers), h)
	}
}

func TestTimeoutBuild(t *testing.T) {
	headers := protocol.NewHeaders().
		WithTimeout(10 * time.Second)

	assert.Equal(t, 10*time.Second, headers.Timeout())

	headers = protocol.NewHeaders().
		WithTimeout(time.Millisecond * 500)

	assert.Equal(t, time.Millisecond*500, headers.Timeout())

	headers = protocol.NewHeaders().
		WithTimeout(time.Minute * 1)
	assert.Equal(t, time.Minute, headers.Timeout())

	headers = protocol.NewHeaders().
		WithTimeout(0)
	assert.Equal(t, 0*time.Second, headers.Timeout())
}

func TestTimeoutBuildOutOfRange(t *testing.T) {
	defValue := 60 * time.Second

	headers := protocol.NewHeaders().
		WithTimeout(time.Minute * 60)
	assert.Equal(t, defValue, headers.Timeout())

	headers = protocol.NewHeaders().
		WithTimeout(-5)
	assert.Equal(t, defValue, headers.Timeout())
}

func TestHeadersRemove(t *testing.T) {
	h := `{
        "response-required": false,
        "correlation-id": "5a1c8922-4828-46f8-9be6-1cbfcfef0d52",
        "content-type": "application/vnd.eclipse.ditto+json",
        "reply-to": "command/t9138cc86fcd145181aa7b_hub",
        "timeout":"30ms",
        "etag": "hash:ba930ee8",
        "If-Match":"hash:ba930ee8",
        "If-None-Match":"hash:ba930ee8",
        "name": "value"
	}`

	var headers protocol.Headers
	require.NoError(t, json.Unmarshal([]byte(h), &headers))

	headers.WithContentType("").
		WithCorrelationID("").
		WithETag("").
		WithIfMatch("").
		WithIfNoneMatch("").
		WithResponseRequired(true).
		WithReplyTo("").
		WithTimeout(0*time.Second).
		WithGeneric("Name", "")

	assert.True(t, headers.ResponseRequired())
	assert.EqualValues(t, 0, headers.Timeout())
	assert.Equal(t, 0, len(headers.ContentType()))
	assert.Equal(t, 0, len(headers.CorrelationID()))
	assert.Equal(t, 0, len(headers.ReplyTo()))
	assert.Equal(t, 0, len(headers.ETag()))
	assert.Equal(t, 0, len(headers.IfMatch()))
	assert.Equal(t, 0, len(headers.IfNoneMatch()))

	_, ok := headers.Generic("name")
	assert.False(t, ok)
}
