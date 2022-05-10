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
	"fmt"
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	thingValue = `"value": {
		"thingId": "org.eclipse.kanto:test",
		"policyId": "org.eclipse.kanto:the_policy_id",
		"definition": "org.eclipse.kanto:Sensor:1.0.0",
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
					"x": 4
				}
			}
		}
	}`

	thingNotFoundErr = `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 404,
			"error": "things:thing.notfound",
			"message": "The Thing with ID 'org.eclipse.kanto:test' could not be found.",
			"description": "Check if the ID of your requested Thing was correct."
		},
		"status": 404
	}`

	createThingCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/create",
		%s,
		"path": "/",
		%s
	}`

	retrieveThingCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/"
	}`

	deleteThingCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/"
	  }`
)

type ThingCommandsSuite struct {
	CommandsSuite
}

func TestThingCommandsSuite(t *testing.T) {
	suite.Run(t, new(ThingCommandsSuite))
}

func (s *ThingCommandsSuite) TestCreateThing() {
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/create",
		%s,
		"path": "/",
		%s,
		"status": 201
	}`
	event := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/created",
		%s,
		"path": "/",
		%s,
		"revision": 1
	}`

	s.handleCommand(withThingValueF(createThingCmd))

	thingOut := model.Thing{}
	s.getThing(&thingOut)
	assert.NotNil(s.T(), thingOut)

	assertPublishedSkipVersioning(s.S(), withHeadersThingValueF(response),
		s.asEnvelope(withHeadersThingValueF(event)))
}

func (s *ThingCommandsSuite) TestCreateThingSkipThingID() {
	command := `{
		"topic": "org.eclipse.kanto/test2/things/twin/commands/create",
		%s,
		"path": "/",
		"value": {
			"policyId": "org.eclipse.kanto:the_policy_id"
		}
	}`
	response := `{
		"topic": "org.eclipse.kanto/test2/things/twin/commands/create",
		%s,
		"path": "/",
		"value": {
			"thingId": "org.eclipse.kanto:test2",
			"policyId": "org.eclipse.kanto:the_policy_id"
		},
		"status": 201
	}`
	event := `{
		"topic": "org.eclipse.kanto/test2/things/twin/events/created",
		%s,
		"path": "/",
		"value": {
			"thingId": "org.eclipse.kanto:test2",
			"policyId": "org.eclipse.kanto:the_policy_id"
		},
		"revision": 1
	}`

	s.handleCommandF(command, defaultHeaders)

	assertPublishedSkipVersioning(s.S(), withResponseHeadersF(response),
		s.asEnvelopeWithID(withResponseHeadersF(event), "org.eclipse.kanto:test2"))
}

func (s *ThingCommandsSuite) TestCreateThingConflictError() {
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 409,
			"error": "things:thing.conflict",
			"message": "The Thing with ID 'org.eclipse.kanto:test' already exists.",
			"description": "Choose another Thing ID."
		},
		"status": 409
	}`

	s.addThing(featuresAsMapValue(s.T(), ""))

	s.handleCommand(withThingValueF(createThingCmd))
	assertPublishedOnErrorF(s.S(), response)
}

func (s *ThingCommandsSuite) TestCreateThingConflictErrorNoResponse() {
	s.addThing(featuresAsMapValue(s.T(), ""))

	s.handleCommand(fmt.Sprintf(createThingCmd, headersNoResponseRequired, thingValue))
	assertPublishedNone(s.S())
}

func (s *ThingCommandsSuite) TestCreateThingNoSettableError() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/create",
		%s,
		"path": "/",
		"value": {
			"thingId": "org.eclipse.kanto:unknown"
		}
	}`
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 400,
			"error": "things:id.notsettable",
			"message": "The Thing ID in the command value is not equal to the Thing ID in the command topic.",
			"description": "Either delete the Thing ID from the command value or use the same Thing ID as in the command topic."
		},
		"status": 400
	}`

	s.handleCommandF(command, defaultHeaders)

	assertPublishedOnErrorF(s.S(), response)
}

func (s *ThingCommandsSuite) TestCreateThingJsonInvalid() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/create",
		%s,
		"path": "/",
		"value": [{
			"test": "org.eclipse.kanto:test"
		}]
	}`
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 400,
			"error": "json.invalid",
			"message": "Failed to parse command value: json: cannot unmarshal array into Go value of type model.Thing.",
			"description": "Check if the JSON was valid and if it was in required format."
		},
		"status": 400
	}`

	s.handleCommandCheckErrorF(command, defaultHeaders)

	assertPublishedOnErrorF(s.S(), response)
}

func (s *ThingCommandsSuite) TestModifyCreateThing() {
	modifyThingCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/",
		%s
	}`
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/",
		%s,
		"status": 201
	}`
	event := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/modified",
		%s,
		"path": "/",
		%s,
		"revision": 1
	}`

	s.handleCommand(withThingValueF(modifyThingCmd))

	thingOut := model.Thing{}
	s.getThing(&thingOut)
	assert.NotNil(s.T(), thingOut)

	assertPublishedSkipVersioning(s.S(), withHeadersThingValueF(response),
		s.asEnvelope(withHeadersThingValueF(event)))
}

func (s *ThingCommandsSuite) TestModifyThing() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/",
		"value": {
			"thingId": "org.eclipse.kanto:test",
			"policyId": "org.eclipse.kanto:the_policy_id",
			"definition": "org.eclipse.kanto:Sensor:221.0.0"
		}
	}`
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/",
		"status": 204
	}`
	event := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/modified",
		%s,
		"path": "/",
		"value": {
			"thingId": "org.eclipse.kanto:test",
			"policyId": "org.eclipse.kanto:the_policy_id",
			"definition": "org.eclipse.kanto:Sensor:221.0.0"
		},
		"revision": 1
	}`

	s.addThing(featuresAsMapValue(s.T(), ""))

	thingOut := model.Thing{}

	s.handleCommandF(command, defaultHeaders)

	s.getThing(&thingOut)
	assert.NotNil(s.T(), thingOut)

	assertPublishedSkipVersioning(s.S(), withHeadersNoResponseRequired(response),
		s.asEnvelopeWithValueF(event))
}

func (s *ThingCommandsSuite) TestModifyThingNoSettableError() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/",
		"value": {
			"thingId": "org.eclipse.kanto:unknown"
		}
	}`
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 400,
			"error": "things:id.notsettable",
			"message": "The Thing ID in the command value is not equal to the Thing ID in the command topic.",
			"description": "Either delete the Thing ID from the command value or use the same Thing ID as in the command topic."
		},
		"status": 400
	}`

	s.handleCommandF(command, defaultHeaders)

	assertPublishedOnErrorF(s.S(), response)
}

func (s *ThingCommandsSuite) TestModifyThingJsonInvalid() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/",
		"value": [{
			"test": "org.eclipse.kanto:test"
		}]
	}`
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 400,
			"error": "json.invalid",
			"message": "Failed to parse command value: json: cannot unmarshal array into Go value of type model.Thing.",
			"description": "Check if the JSON was valid and if it was in required format."
		},
		"status": 400
	}`

	s.handleCommandCheckErrorF(command, defaultHeaders)

	assertPublishedOnErrorF(s.S(), response)
}

func (s *ThingCommandsSuite) TestRetrieveThing() {
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/",
		%s,
		"status": 200
	}`
	features := `{
		"meter": {
			"properties": {
				"x": 12.34,
				"y": 5.6
			},
			"desiredProperties": {
				"x": 4
			}
		}
	}`
	attributes := `{
		"test": {
			"package": "commands",
			"version": 1.0
		}
	}`

	thing := (&model.Thing{}).
		WithIDFrom(testThingID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:Sensor:1.0.0").
		WithAttributes(asMapValue(s.T(), attributes)).
		WithFeatures(featuresAsMapValue(s.T(), features))
	s.createThing(thing)

	s.handleCommandF(retrieveThingCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), s.asEnvelope(withHeadersThingValueF(response)))
}

func (s *ThingCommandsSuite) TestRetrieveThingWithFields() {
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/",
		%s,
		"status": 200
	}`
	features := `{
		"meter": {
			"properties": {
				"x": 12.34
			}
		}
	}`
	attributes := `{
		"test": {
			"package": "commands",
			"version": 1.0
		}
	}`

	thing := (&model.Thing{}).
		WithIDFrom(testThingID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:Sensor:1.0.0").
		WithAttributes(asMapValue(s.T(), attributes)).
		WithFeatures(featuresAsMapValue(s.T(), features))
	s.createThing(thing)

	retrieveThingWithFieldsCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"fields": "thingId,attributes,features/test"
	}`

	s.handleCommandF(retrieveThingWithFieldsCmd, defaultHeaders)

	thingWithFieldsValue := `"value": {
		"thingId": "org.eclipse.kanto:test",
		"attributes": {
			"test": {
				"package": "commands",
				"version": 1.0
			}
		}
	}`
	assertPublishedSkipVersioning(
		s.S(), s.asEnvelope(fmt.Sprintf(response, responseHeaders, thingWithFieldsValue)))
}

func (s *ThingCommandsSuite) TestRetrieveThingNotFoundError() {
	s.handleCommandF(retrieveThingCmd, defaultHeaders)
	assertPublishedOnErrorF(s.S(), thingNotFoundErr)
}

func (s *ThingCommandsSuite) TestRetrieveThingWithInvalidFields() {
	retrieveThingWithFieldCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"fields": "thingId("
	}`
	thing := (&model.Thing{}).
		WithIDFrom(testThingID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:Sensor:1.0.0")
	s.createThing(thing)

	fieldSelectorErr := `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 400,
			"error": "json.fieldselector.invalid",
			"message": "Invalid field selector: the field selector 'thingId(' is with different amount of opening '(' and closing ')' parentheses.",
			"description": "Check fields syntax."
		},
		"status": 400
	}`

	s.handleCommandF(retrieveThingWithFieldCmd, defaultHeaders)
	assertPublishedOnErrorF(s.S(), fieldSelectorErr)
}

func (s *ThingCommandsSuite) TestRetrieveThingWithInvalidFieldsNoResponse() {
	retrieveThingWithFieldCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"fields": "thingId("
	}`
	thing := (&model.Thing{}).
		WithIDFrom(testThingID)
	s.createThing(thing)

	s.handleCommandF(retrieveThingWithFieldCmd, responseHeaders)
	assertPublishedNone(s.S())
}

func (s *ThingCommandsSuite) TestDeleteThing() {
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/",
		"status": 204
	}`
	event := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/deleted",
		%s,
		"path": "/",
		"revision": 1
	}`

	s.addTestThing()

	s.handleCommandF(deleteThingCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), withHeadersNoResponseRequired(response),
		s.asEnvelopeNoValueF(event))
}

func (s *ThingCommandsSuite) TestDeleteThingNotFoundError() {
	s.handleCommandF(deleteThingCmd, defaultHeaders)
	assertPublishedOnErrorF(s.S(), thingNotFoundErr)
}

func withThingValueF(envF string) string {
	return fmt.Sprintf(envF, defaultHeaders, thingValue)
}

func withHeadersThingValueF(envF string) string {
	return fmt.Sprintf(envF, responseHeaders, thingValue)
}
