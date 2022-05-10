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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	deleteFeaturesCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features"
	}`
	expDeleteFeaturesRsp = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features",
		"status": 204
	}`
	retrieve = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/features"
	}`

	featuresNotFoundErr = `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 404,
			"error": "things:features.notfound",
			"message": "The Features on the Thing with ID 'org.eclipse.kanto:test' do not exist.",
			"description": "Check if the ID of the Thing was correct."
		},
		"status": 404
	}`
)

type FeaturesCommandsSuite struct {
	CommandsSuite
}

func TestFeaturesCommandsSuite(t *testing.T) {
	suite.Run(t, new(FeaturesCommandsSuite))
}

func (s *FeaturesCommandsSuite) TestThingFeaturesModify() {
	type featuresTest struct {
		input    string
		command  string
		output   string
		response string
		event    string
	}

	expResponse := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features",
		"status": 204
	}`

	tests := []featuresTest{
		// add new features
		{
			input: `{"tempFeature": {"properties": {"x": 1.2}}}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features",
				"value": {
					"meter": {
						"properties": {
							"x": 3.141,
							"y": 2.718
						},
						"desiredProperties": {
							"x": 4,
							"y": 3
						}
					}
				}
			}`,
			output: `{"meter": {"properties": {"x": 3.141, "y": 2.718},
					"desiredProperties": {"x": 4, "y": 3}}}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features",
				"value": {
					"meter": {
						"properties": {
							"x": 3.141,
							"y": 2.718
						},
						"desiredProperties": {
							"x": 4,
							"y": 3
						}
					}
				}
			}`,
		},

		// no features and add 2 features
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features",
				"value": {
					"meter": { "properties": {
							"x1": 1.0,
							"x2": "2.0"
						}
					},
					"temperature": {"properties": {
							"y": 3.3
						}
					}
				}
			}`,
			output: `{"meter": {"properties": {"x1": 1.0, "x2": "2.0"}}, "temperature": {"properties": {"y": 3.3}}}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features",
				"status": 201
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/created",
				%s,
				"path": "/features",
				"value": {
					"meter": {"properties": {
							"x1": 1.0,
							"x2": "2.0"
						}
					},
					"temperature": {"properties": {
							"y": 3.3
						}
					}
				}
			}`,
		},

		// change properties of one feature and add new feature
		{
			input: `{"meter": {"properties": {"x": 1.2}}}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features",
				"value": {
					"meter": {"properties": {
							"x": {
								"x1": 1.0,
								"x2": 2.0
							}
						}
					},
					"temperature": {"properties": {
							"y1": 1.2
						}
					}
				}
			}`,
			output:   `{"meter": {"properties": {"x": {"x1": 1.0, "x2": 2.0}}}, "temperature": {"properties": {"y1": 1.2}}}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features",
				"value": {
					"meter": {"properties": {
							"x": {
								"x1": 1.0,
								"x2": 2.0
							}
						}
					},
					"temperature": {"properties": {
							"y1": 1.2
						}
					}
				}
			}`,
		},

		// modify existing with empty
		{
			input: `{"meter": {"properties": {"x1": 11.2}}, "temperature": {"properties": {"y": 30.0}}}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features",
				"value": {}
			}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features",
				"value": {}
			}`,
		},
	}

	thingOut := model.Thing{}
	revision := thingOut.Revision - 1
	for _, test := range tests {
		featuresIn := featuresAsMapValue(s.T(), test.input)
		s.addThing(featuresIn)

		formattedCommand := withDefaultHeadersF(test.command)
		outputMsgs := s.handleCommand(formattedCommand)
		assert.Nil(s.T(), outputMsgs)
		s.getThing(&thingOut)

		featuresOutput := featuresAsMapValue(s.T(), test.output)
		assert.EqualValues(s.T(), featuresOutput, thingOut.Features)
		revision = revision + 2 // 2 storage writings expected
		assert.EqualValues(s.T(), revision, thingOut.Revision)

		assertPublishedOnOkF(s.S(), test.response, test.event)

		// check that published hono message is with false value for response required header
		outputEnv := assertHonoMsgPublished(s.S())
		assertEnvelopeDataResponseRequiredChanged(s.S(),
			s.asEnvelope(formattedCommand), outputEnv, true)

	}
}

func (s *FeaturesCommandsSuite) TestThingFeaturesRetrieve() {

	input := `{"tempFeature": {"properties": {"x": 1.2}}}`
	retrieveRsp := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/features",
		"value": {"tempFeature": {"properties": {"x": 1.2}}},
		"status": 200
	}`

	s.addThing(featuresAsMapValue(s.T(), input))

	s.handleRetrieveCheckResponseF(retrieve, retrieveRsp)
}

func (s *FeaturesCommandsSuite) TestThingFeaturesRetrieveNoResponse() {
	s.addThing(featuresAsMapValue(s.T(), ""))

	s.handleCommandF(retrieve, headersNoResponseRequired)
	assertPublishedNone(s.S())
}

func (s *FeaturesCommandsSuite) TestThingFeaturesModifyInvalidValue() {
	// invalid json response
	command := `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features",
				"value": 5
			}`

	s.addThing(nil)
	s.handleCommandCheckErrorF(command, defaultHeaders)

}

func (s *FeaturesCommandsSuite) TestThingFeaturesDelete() {
	type featuresTest struct {
		input    string
		response string
		event    string
	}

	deleteEvent := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/deleted",
		%s,
		"path": "/features"
	}`

	tests := []featuresTest{
		// no features and try to delete
		{
			response: featuresNotFoundErr,
		},

		// delete features
		{
			input: `{"meter": {"properties": {"x": 3.141, "y": 2.718},
					"desiredProperties": {"x": 4, "y": 3}}}`,
			response: expDeleteFeaturesRsp,
			event:    deleteEvent,
		},

		// thing have 2 features and delete all features
		{
			input:    `{"meter": {"properties": {"x": {"x1": 1.0, "x2": 2.0}}}, "temperature": {"properties": {"y1": 1.2}}}`,
			response: expDeleteFeaturesRsp,
			event:    deleteEvent,
		},
	}

	thingOut := model.Thing{}

	for _, test := range tests {
		featuresIn := featuresAsMapValue(s.T(), test.input)
		s.addThing(featuresIn)

		deleteFeaturesCommand := withDefaultHeadersF(deleteFeaturesCmd)
		outputMsgs := s.handleCommand(deleteFeaturesCommand)
		assert.Nil(s.T(), outputMsgs)
		thingOut.WithFeatures(nil)
		s.getThing(&thingOut)
		assert.Nil(s.T(), thingOut.Features)

		if len(test.event) == 0 {
			assertPublishedOnErrorF(s.S(), test.response)
		} else {
			assertPublishedOnDeletedF(s.S(), test.response, test.event)
		}
		// check that published hono message is with false value for response required header
		outputEnv := assertHonoMsgPublished(s.S())
		assertEnvelopeDataResponseRequiredChanged(s.S(),
			s.asEnvelope(deleteFeaturesCommand), outputEnv, true)
	}

	// try to get features from thing without features
	s.handleRetrieveCheckResponseF(retrieve, featuresNotFoundErr)
}

func (s *FeaturesCommandsSuite) TestDeleteFeaturesFromNotCreatedThing() {
	s.handleCommandF(deleteFeaturesCmd, defaultHeaders)
	thingOut := model.Thing{}
	err := s.handler.Storage.GetThing(testThingID, &thingOut)
	assert.NotNil(s.T(), err)
	assertHonoMsgPublished(s.S())
}

func (s *FeaturesCommandsSuite) TestRetrieveFeaturesThingNotFoundError() {
	s.handleRetrieveCheckResponseF(retrieve, thingNotFoundErr)
}

func (s *FeaturesCommandsSuite) TestRetrieveFeaturesThingNotFoundErrorNoResponse() {
	s.handleCommandF(retrieve, headersNoResponseRequired)
	assertPublishedNone(s.S())
}

func (s *FeaturesCommandsSuite) TestModifyFeatures() {
	// modify features when no thing
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features",
		"value": {
			"meter": {
				"properties": {
					"x": 3.141,
					"y": 2.718
				}
			}
		}
	}`

	s.deleteThing()

	s.handleCommandF(command, defaultHeaders)
	assertPublishedOnErrorF(s.S(), thingNotFoundErr)
	assertHonoMsgPublished(s.S())
}
