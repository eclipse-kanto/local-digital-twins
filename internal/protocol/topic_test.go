// Copyright (c) 2021 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0
//
// SPDX-License-Identifier: EPL-2.0

package protocol_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
)

func TestTopicPolicies(t *testing.T) {
	test := `"org.eclipse.kanto/test/policies/commands/create"`

	var topic protocol.Topic
	require.NoError(t, json.Unmarshal([]byte(test), &topic))

	assert.Equal(t, "org.eclipse.kanto", topic.Namespace)
	assert.Equal(t, "test", topic.EntityID)
	assert.Equal(t, protocol.GroupPolicies, topic.Group)
	assert.Equal(t, 0, len(topic.Channel))
	assert.Equal(t, protocol.CriterionCommands, topic.Criterion)
	assert.Equal(t, protocol.ActionCreate, topic.Action)

	assert.Equal(t, test, fmt.Sprintf("%q", topic.String()))

	mTopic, err := topic.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, test, string(mTopic))
}

func TestTopicThingsWithAction(t *testing.T) {
	test := `"org.eclipse.kanto/test/things/twin/commands/modify"`

	var topic protocol.Topic
	require.NoError(t, json.Unmarshal([]byte(test), &topic))

	assert.Equal(t, "org.eclipse.kanto", topic.Namespace)
	assert.Equal(t, "test", topic.EntityID)
	assert.Equal(t, protocol.GroupThings, topic.Group)
	assert.Equal(t, protocol.ChannelTwin, topic.Channel)
	assert.Equal(t, protocol.CriterionCommands, topic.Criterion)
	assert.Equal(t, protocol.ActionModify, topic.Action)

	assert.Equal(t, test, fmt.Sprintf("%q", topic.String()))
}

func TestTopicThingsNoAction(t *testing.T) {
	test := `"org.eclipse.kanto/test/things/twin/errors"`

	var topic protocol.Topic
	require.NoError(t, json.Unmarshal([]byte(test), &topic))

	assert.Equal(t, "org.eclipse.kanto", topic.Namespace)
	assert.Equal(t, "test", topic.EntityID)
	assert.Equal(t, protocol.GroupThings, topic.Group)
	assert.Equal(t, protocol.ChannelTwin, topic.Channel)
	assert.Equal(t, protocol.CriterionErrors, topic.Criterion)
	assert.Equal(t, 0, len(topic.Action))

	assert.Equal(t, test, fmt.Sprintf("%q", topic.String()))
}

func TestTopicValidNamespace(t *testing.T) {
	tests := []string{
		`"org.eclipse.kanto/test/things/twin/retrieve"`,
		`"_/test/things/twin/retrieve"`,
		`"org.eclipse.kanto/_/things/twin/retrieve"`,
		`"_/_/things/twin/retrieve"`,
	}

	var topic protocol.Topic
	for _, test := range tests {
		assert.NoError(t, json.Unmarshal([]byte(test), &topic), test)
		assert.NotEmpty(t, topic.Namespace, test)
		assert.NotEmpty(t, topic.EntityID, test)
	}
}

func TestTopicInvalidNamespace(t *testing.T) {
	tests := []string{
		`"org.eclipse.kanto//test/things/twin/errors"`,
		`"org.eclipse.kant§o/test/things/twin/errors"`,
		`"org.eclipse.kanto/test-f§o/things/twin/errors"`,
	}

	var topic protocol.Topic
	for _, test := range tests {
		assert.Error(t, json.Unmarshal([]byte(test), &topic), test)
		assert.Empty(t, topic.Namespace, test)
		assert.Empty(t, topic.EntityID, test)
	}
}

func TestTopicThingsBuild(t *testing.T) {
	topic := (&protocol.Topic{}).
		WithNamespace("org.eclipse.kanto").
		WithEntityID("test").
		WithGroup(protocol.GroupThings).
		WithChannel(protocol.ChannelTwin).
		WithCriterion(protocol.CriterionErrors)
	assert.Equal(t, "org.eclipse.kanto/test/things/twin/errors", topic.String())

	topic.
		WithCriterion(protocol.CriterionEvents).
		WithAction(protocol.ActionModified)
	assert.Equal(t, "org.eclipse.kanto/test/things/twin/events/modified", topic.String())

	topic.
		WithCriterion(protocol.CriterionErrors).
		WithAction("")
	assert.Equal(t, "org.eclipse.kanto/test/things/twin/errors", topic.String())
}
