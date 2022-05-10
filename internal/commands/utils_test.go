// Copyright (c) 2022 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0
//
// SPDX-License-Identifier: EPL-2.0

package commands_test

import (
	"encoding/json"
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/commands"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathParse(t *testing.T) {
	type topicTest struct {
		cmdPath string
		scope   commands.Scope
		target  string
		path    string
	}
	tests := []topicTest{
		{
			cmdPath: "/",
			scope:   commands.ScopeThing,
		},
		{
			cmdPath: "/attributes",
			scope:   commands.ScopeAttributes,
		},
		{
			cmdPath: "/attributes/attr1/field",
			scope:   commands.ScopeAttributes,
			target:  "/attr1/field",
		},
		{
			cmdPath: "/definition",
			scope:   commands.ScopeDefinition,
		},
		{
			cmdPath: "/policyId",
			scope:   commands.ScopePolicy,
		},
		{
			cmdPath: "/features",
			scope:   commands.ScopeFeatures,
		},
		{
			cmdPath: "/features/meter",
			scope:   commands.ScopeFeature,
			target:  "meter",
		},
		{
			cmdPath: "/features/meter/definition",
			scope:   commands.ScopeFeatureDefinition,
			target:  "meter",
		},
		{
			cmdPath: "/features/meter/properties",
			scope:   commands.ScopeFeatureProperties,
			target:  "meter",
		},
		{
			cmdPath: "/features/meter/properties/prop/field",
			scope:   commands.ScopeFeatureProperty,
			target:  "meter",
			path:    "/prop/field",
		},
		{
			cmdPath: "/features/meter/desiredProperties",
			scope:   commands.ScopeFeatureDesiredProperties,
			target:  "meter",
		},
		{
			cmdPath: "/features/meter/desiredProperties/prop/field",
			scope:   commands.ScopeFeatureDesiredProperty,
			target:  "meter",
			path:    "/prop/field",
		},
	}
	for _, test := range tests {
		cmd, id, path := commands.ParseCmdPath(test.cmdPath)
		assert.EqualValues(t, test.scope, cmd)
		assert.EqualValues(t, test.target, id)
		assert.EqualValues(t, test.path, path)
	}
}

func TestPathParseInvalid(t *testing.T) {
	tests := []string{
		"/unknown", "/attributes_", "/definitionA",
		"/policyId_!", "/features.",
		"/features/meter/unknown", "/features/meter/unknown/prop/field",
	}
	for _, test := range tests {
		cmd, target, path := commands.ParseCmdPath(test)
		assert.EqualValues(t, commands.ScopeUnknown, cmd)
		assert.EqualValues(t, "", target)
		assert.EqualValues(t, "", path)
	}
}

func TestEventPublishTopic(t *testing.T) {
	type topics struct {
		envTopic  string
		mqttTopic string
	}

	deviceID := "org.eclipse.kanto:test"
	tests := []topics{
		{
			envTopic:  `"org.eclipse.kanto/test/things/twin/events/modified"`,
			mqttTopic: "command///req//modified",
		},
		{
			envTopic:  `"org.eclipse.kanto/test/things/twin/events/deleted"`,
			mqttTopic: "command///req//deleted",
		},
		{
			envTopic:  `"org.eclipse.kanto/test:temp/things/twin/events/modified"`,
			mqttTopic: "command//org.eclipse.kanto:test:temp/req//modified",
		},
		{
			envTopic:  `"org.eclipse.kanto/test:temp/things/twin/events/deleted"`,
			mqttTopic: "command//org.eclipse.kanto:test:temp/req//deleted",
		},
	}

	topic := &protocol.Topic{}
	for _, test := range tests {
		assert.NoError(t, json.Unmarshal([]byte(test.envTopic), topic))
		assert.Equal(t, protocol.CriterionEvents, topic.Criterion)
		assert.Equal(t, test.mqttTopic, commands.EventPublishTopic(deviceID, topic))
	}
}

func TestResponsePublishTopic(t *testing.T) {
	type topics struct {
		envTopic  string
		mqttTopic string
	}

	deviceID := "org.eclipse.kanto:test"
	tests := []topics{
		{
			envTopic:  `"org.eclipse.kanto/test/things/twin/commands/modify"`,
			mqttTopic: "command///req//modify-response",
		},
		{
			envTopic:  `"org.eclipse.kanto/test/things/twin/commands/delete"`,
			mqttTopic: "command///req//delete-response",
		},
		{
			envTopic:  `"org.eclipse.kanto/test/things/twin/errors"`,
			mqttTopic: "command///req//errors-response",
		},
		{
			envTopic:  `"org.eclipse.kanto/test:temp/things/twin/commands/modify"`,
			mqttTopic: "command//org.eclipse.kanto:test:temp/req//modify-response",
		},
		{
			envTopic:  `"org.eclipse.kanto/test:temp/things/twin/commands/delete"`,
			mqttTopic: "command//org.eclipse.kanto:test:temp/req//delete-response",
		},
		{
			envTopic:  `"org.eclipse.kanto/test:temp/things/twin/errors"`,
			mqttTopic: "command//org.eclipse.kanto:test:temp/req//errors-response",
		},
	}

	topic := &protocol.Topic{}
	for _, test := range tests {
		assert.NoError(t, json.Unmarshal([]byte(test.envTopic), topic))
		assert.Equal(t, test.mqttTopic, commands.ResponsePublishTopic(deviceID, topic))
	}
}

func TestCommandResponse(t *testing.T) {
	type cmdResponseTest struct {
		command  string
		response string
	}
	tests := []cmdResponseTest{
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
				%s,
				"path": "/"
			}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
				%s,
				"path": "/",
				"value": {
				  "thingId": "org.eclipse.kanto:test",
				  "policyId": "org.eclipse.kanto:the_policy_id",
				  "attributes": {
					"test": {
						"package": "commands",
						"version": 1.0
					}
				  },
				  "features": {
					"meter": {
					  "properties": {
						"x": 12.34,
						"y": 5.6
					  },
					  "desiredProperties": {
						"x": 4,
						"y": 3
					  }
					}
				  }
				},
				"status": 200
			  }`,
		},
	}

	thing := createThing(t)
	for _, test := range tests {
		commandEnv := &protocol.Envelope{}
		assert.NoError(t, json.Unmarshal([]byte(withDefaultHeadersF(test.command)), commandEnv))

		responseEnv := commands.ResponseEnvelopeWithValue(commandEnv, 200, thing)

		expResponse := &protocol.Envelope{}
		assert.NoError(t, json.Unmarshal([]byte(withResponseHeadersF(test.response)), expResponse))

		assert.EqualValues(t, expResponse.Topic, responseEnv.Topic)
		assert.EqualValues(t, expResponse.Headers, responseEnv.Headers)
		assert.EqualValues(t, expResponse.Path, responseEnv.Path)

		assert.EqualValues(t, asThing(t, expResponse.Value), asThing(t, responseEnv.Value))
	}
}

func createThing(t *testing.T) *model.Thing {
	data := `{
		"thingId": "org.eclipse.kanto:test",
		"policyId": "org.eclipse.kanto:the_policy_id",
		"attributes": {
		  "test": {
			"package": "commands",
			"version": 1.0
		  }
		},
		"features": {
		  "meter": {
			"properties": {
			  "x": 12.34,
			  "y": 5.6
			},
			"desiredProperties": {
			  "x": 4,
			  "y": 3
			}
		  }
		}
	  }`

	return asThing(t, []byte(data))
}

func asThing(t *testing.T, data []byte) *model.Thing {
	thing := &model.Thing{}
	err := json.Unmarshal(data, thing)
	require.NoError(t, err)
	return thing
}
