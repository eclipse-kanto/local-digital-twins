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
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/stretchr/testify/suite"
)

type ThingsCommandsSuite struct {
	CommandsSuite
}

func TestThingsCommandsSuite(t *testing.T) {
	suite.Run(t, new(ThingsCommandsSuite))
}

func (s *ThingsCommandsSuite) TestRetrieveMultipleThings() {
	retrieveAllThingsCmd := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": {
			"thingIds": [
				"org.eclipse.kanto:test",
				"org.eclipse.kanto:testSensor"
			]
		}
	}`

	response := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": [
			{
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
							"x": 12.34
						}
					}
				}
			},
			{
				"thingId": "org.eclipse.kanto:testSensor",
				"policyId": "org.eclipse.kanto:the_policy_id",
				"definition": "org.eclipse.kanto:testSensor:2.0.0",
				"attributes": {
					"testAttributes": {
						"attribute1": "attributeValue"
					}
				},
				"features": {
					"testmeter": {
						"properties": {
							"testProp": "test123"
						}
					}
				}
			}
		],
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
	testFeatures := `{
		"testmeter": {
			"properties": {
				"testProp": "test123"
			}
		}
	}`
	testAttributes := `{
		"testAttributes": {
			"attribute1": "attributeValue"
		}
	}`

	thing := (&model.Thing{}).
		WithIDFrom(testThingID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:Sensor:1.0.0").
		WithAttributes(asMapValue(s.T(), attributes)).
		WithFeatures(featuresAsMapValue(s.T(), features))
	s.createThing(thing)

	testSensorThingID := "org.eclipse.kanto:testSensor"
	thingSensor := (&model.Thing{}).
		WithIDFrom(testSensorThingID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:testSensor:2.0.0").
		WithAttributes(asMapValue(s.T(), testAttributes)).
		WithFeatures(featuresAsMapValue(s.T(), testFeatures))
	s.createThing(thingSensor)

	s.handleCommandF(retrieveAllThingsCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), s.asEnvelopeWithValueF(response))
	s.deleteCreatedThing(testSensorThingID)
}

func (s *ThingsCommandsSuite) TestRetrieveMultipleThingsWithFields() {
	retrieveAllThingsCmd := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": {
			"thingIds": [
				"org.eclipse.kanto:test",
				"org.eclipse.kanto:testSensor"
			]
		},
		"fields": "thingId,policyId,definition,attributes,features/testmeter"
	}`

	response := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": [
			{
				"thingId": "org.eclipse.kanto:test",
				"policyId": "org.eclipse.kanto:the_policy_id",
				"definition": "org.eclipse.kanto:Sensor:1.0.0",
				"attributes": {
					"test": {
						"package": "commands",
						"version": 1.0
					}
				}
			},
			{
				"thingId": "org.eclipse.kanto:testSensor",
				"policyId": "org.eclipse.kanto:the_policy_id",
				"definition": "org.eclipse.kanto:testSensor:2.0.0",
				"attributes": {
					"testAttributes": {
						"attribute1": "attributeValue"
					}
				},
				"features": {
					"testmeter": {
						"properties": {
							"testProp": "test123"
						}
					}
				}
			}
		],
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
	testFeatures := `{
		"testmeter": {
			"properties": {
				"testProp": "test123"
			}
		}
	}`
	testAttributes := `{
		"testAttributes": {
			"attribute1": "attributeValue"
		}
	}`

	thing := (&model.Thing{}).
		WithIDFrom(testThingID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:Sensor:1.0.0").
		WithAttributes(asMapValue(s.T(), attributes)).
		WithFeatures(featuresAsMapValue(s.T(), features))
	s.createThing(thing)

	testSensorThingID := "org.eclipse.kanto:testSensor"
	thingSensor := (&model.Thing{}).
		WithIDFrom(testSensorThingID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:testSensor:2.0.0").
		WithAttributes(asMapValue(s.T(), testAttributes)).
		WithFeatures(featuresAsMapValue(s.T(), testFeatures))
	s.createThing(thingSensor)

	s.handleCommandF(retrieveAllThingsCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), s.asEnvelopeWithValueF(response))
	s.deleteCreatedThing(testSensorThingID)
}

func (s *ThingsCommandsSuite) TestRetrieveMultipleThingsInvalidJSONError() {
	retrieveAllThingsCmd := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": {
			"test": []
		}
	}`

	response := `{
		"topic": "_/_/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 400,
			"error": "json.invalid",
			"message": "Failed to parse command value: Empty 'thingIds' value.",
			"description": "Check if the JSON was valid and if it was in required format."
		},
		"status": 400
	}`
	s.handleCommandF(retrieveAllThingsCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), withResponseHeadersF(response))
}

func (s *ThingsCommandsSuite) TestRetrieveMultipleThingsIDsNotArrayValue() {
	retrieveAllThingsCmd := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": {
			"thingIds": "org.eclipse.kanto:test"
		}
	}`

	response := `{
		"topic": "_/_/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 400,
			"error": "json.invalid",
			"message": "Failed to parse command value: json: cannot unmarshal string into Go value of type []string.",
			"description": "Check if the JSON was valid and if it was in required format."
		},
		"status": 400
	}`
	s.handleCommandF(retrieveAllThingsCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), withResponseHeadersF(response))
}

func (s *ThingsCommandsSuite) TestRetrieveMultipleThingsInvalidError() {
	retrieveAllThingsCmd := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": {
			"thingIds": [
				"org.eclipse.kanto:test",
				"org.eclipse.kanto"
			]
		}
	}`

	response := `{
		"topic": "_/_/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 400,
			"error": "things:id.invalid",
			"message": "Thing ID 'org.eclipse.kanto' is not valid!",
			"description": "It must conform to the namespaced entity ID notation (see Ditto documentation)"
		},
		"status": 400
	}`

	s.handleCommandF(retrieveAllThingsCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), withResponseHeadersF(response))
}

func (s *ThingsCommandsSuite) TestRetrieveMultipleThingsInvalidFields() {
	retrieveAllThingsCmd := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": {
			"thingIds": [
				"org.eclipse.kanto:test",
				"org.eclipse.kanto:Sensor"
			]
		},
		"fields": "thingId("
	}`

	response := `{
		"topic": "_/_/things/twin/errors",
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

	thing := (&model.Thing{}).
		WithIDFrom(testThingID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:Sensor:1.0.0")
	s.createThing(thing)

	thingSensorID := "org.eclipse.kanto:Sensor"
	thingSensor := (&model.Thing{}).
		WithIDFrom(thingSensorID).
		WithPolicyIDFrom("org.eclipse.kanto:the_policy_id").
		WithDefinitionFrom("org.eclipse.kanto:Sensor:1.0.0")
	s.createThing(thingSensor)

	s.handleCommandF(retrieveAllThingsCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), withResponseHeadersF(response))
	s.deleteCreatedThing(thingSensorID)
}

func (s *ThingsCommandsSuite) TestRetrieveMultipleThingsWithNotExistingIDs() {
	retrieveAllThingsCmd := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": {
			"thingIds": [
				"org.eclipse.kanto:testNotExisting",
				"org.eclipse.kanto:testSensorNotExisting"
			]
		}
	}`

	response := `{
		"topic": "_/_/things/twin/commands/retrieve",
		%s,
		"path": "/",
		"value": [],
		"status": 200
	}`

	s.handleCommandF(retrieveAllThingsCmd, defaultHeaders)
	assertPublishedSkipVersioning(s.S(), withResponseHeadersF(response))
}
