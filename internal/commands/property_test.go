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
	retrieveXCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/features/meter/properties/x"
	}`
	retrieveDesiredXCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/features/meter/desiredProperties/x"
	}`

	propertyNotFoundErr = `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 404,
			"error": "things:feature.property.notfound",
			"message": "The property with JSON Pointer '/x' of the Feature with ID 'meter' on the Thing with ID 'org.eclipse.kanto:test' does not exist.",
			"description": "Check if the ID of the Thing, the Feature ID and the key of your requested property was correct."
		},
		"status": 404
	}`
)

type PropertyCommandsSuite struct {
	CommandsSuite
}

func TestPropertyCommandsSuite(t *testing.T) {
	suite.Run(t, new(PropertyCommandsSuite))
}

func (s *PropertyCommandsSuite) TestPropertyModify() {
	s.addTestThing()
	type propertyTest struct {
		input       string
		command     string
		output      string
		response    string
		event       string
		retrieveRsp string
	}

	expResponse := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter/properties/x",
		"status": 204
	}`

	expEvent := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/modified",
		%s,
		"path": "/features/meter/properties/x",
		"value":  {
			"x1": 1.0,
			"x2": 2.0
		}
	}`

	tests := []propertyTest{
		// add property
		{
			input: `{"x": 1.2}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/y",
				"value": 3.4
			}`,
			output: `{"x": 1.2, "y": 3.4}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/y",
				"status": 204
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties/y",
				"value": 3.4
			}`,
		},

		// modify existing property
		{
			input: `{"x": 1.2, "y": 3.4}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/x",
				"value": 5.0
			}`,
			output:   `{"x": 5.0, "y": 3.4}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties/x",
				"value": 5.0
			}`,
		},

		// create properties and add new
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/x",
				"value": {
					"x1": 1.0,
					"x2": 2.0
				}
			}`,
			output: `{"x": {"x1": 1.0, "x2": 2.0}}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/x",
				"status": 201
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/created",
				%s,
				"path": "/features/meter/properties/x",
				"value":  {
					"x1": 1.0,
					"x2": 2.0
				}
			}`,
		},

		// modify from numeric to object
		{
			input: `{"x": 1.2}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/x",
				"value": {
					"x1": 1.0,
					"x2": 2.0
				}
			}`,
			output:   `{"x":{"x1": 1.0, "x2": 2.0}}`,
			response: expResponse,
			event:    expEvent,
		},

		// add nonexisting path elements
		{
			input: `{"y": 3.4}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/x/x1",
				"value": {
					"x1.1": 1.1,
					"x1.2": 1.2
				}
			}`,
			output: `{"x":{"x1": {"x1.1": 1.1, "x1.2": 1.2}}, "y": 3.4}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/x/x1",
				"status": 204
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties/x/x1",
				"value":  {
					"x1.1": 1.1,
					"x1.2": 1.2
				}
			}`,
			retrieveRsp: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
				%s,
				"path": "/features/meter/properties/x",
				"value":  {
					"x1": {
						"x1.1": 1.1,
						"x1.2": 1.2
					}
				},
				"status": 200
			}`,
		},

		// keep existing and modify another from numeric to object
		{
			input: `{"x": 0.0, "y": 3.0}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/x",
				"value": {
					"x1": 1.0,
					"x2": 2.0
				}
			}`,
			output:   `{"x": {"x1": 1.0, "x2": 2.0}, "y": 3.0}`,
			response: expResponse,
			event:    expEvent,
		},

		// modify array value
		{
			input: `{"foo": ["first", "second", {"nested2": {"value2": 15}}, ["fifth", "sixth"], "fourth"]}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/foo/2/nested2/value2",
				"value": "qux"
			}`,
			output: `{"foo": ["first", "second", {"nested2": {"value2": "qux"}}, ["fifth", "sixth"], "fourth"]}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/foo/2/nested2/value2",
				"status": 204
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties/foo/2/nested2/value2",
				"value": "qux"
			}`,
			retrieveRsp: propertyNotFoundErr,
		},

		// modify existing with creation of nested nonexisting path elements
		// err="encountered value collision whilst building path" path=/features/meter/properties/x/x1
		// {
		// 	input: `{"x": 10.0, "y": 20.0}`,
		// 	command: `{
		// 		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		// 		%s,
		// 		"path": "/features/meter/properties/x/x1",
		// 		"value": {
		// 			"x1.1": 11.1,
		// 			"x1.2": 11.2
		// 		}
		// 	}`,
		// 	output: `{"x":{"x1": {"x1.1": 11.1, "x1.2": 11.2}}, "y": 20.0}`,
		// },
	}

	featureIn := model.Feature{}
	featureOut := model.Feature{}

	for _, test := range tests {
		properties := asMapValue(s.T(), test.input)
		s.addFeature(testFeatureID, featureIn.WithProperties(properties))

		s.handleCommandF(test.command, defaultHeaders)
		assertPublishedOnOkF(s.S(), test.response, test.event)

		s.getFeature(testFeatureID, &featureOut)
		assert.NotNil(s.T(), featureOut.Properties)

		properties = asMapValue(s.T(), test.output)
		assert.EqualValues(s.T(), properties, featureOut.Properties)

		s.handleRetrieveCheckResponseF(retrieveXCmd, test.retrieveRsp)
	}
}

func (s *PropertyCommandsSuite) TestPropertyDelete() {
	s.addTestThing()
	type propertyTest struct {
		input    string
		command  string
		output   string
		response string
		path     string
	}
	deletedEvent := withHeadersNoResponseRequired(`{
		"topic": "org.eclipse.kanto/test/things/twin/events/deleted",
		%s,
		"path": "/features/meter/properties/x"
	}`)
	deleteX := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter/properties/x"
	}`
	tests := []propertyTest{
		// delete existing property on top level
		{
			input:   `{"x":{"x1": {"x1.1": 1.1, "x1.2": 1.2}}, "y": 3.0}`,
			command: deleteX,
			output:  `{"y": 3.0}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/x",
				"status": 204
			}`,
		},

		// delete the only property
		{
			input:   `{"x":{"x1": 1.0, "x2": 2.0}}`,
			command: deleteX,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/x",
				"status": 204
			}`,
		},

		// delete existing property on internal level
		{
			input: `{"x": {"x1": 1.0, "x2": 2.0}}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/x/x1"
			}`,
			output: `{"x": {"x2": 2.0}}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/x/x1",
				"status": 204
			}`,
			path: `/features/meter/properties/x/x1`,
		},

		// delete internal subjson
		{
			input: `{"x": {"x1": {"x1.1": {"x1.1.1": {"x1.1.1.1": 11.1, "x1.2": 11.2}}, "x1.2": 11.2}}, "y": 20.0}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/x/x1/x1.1/x1.1.1"
			}`,
			output: `{"x": {"x1": {"x1.1": {}, "x1.2": 11.2}}, "y": 20.0}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/x/x1/x1.1/x1.1.1",
				"status": 204
			}`,
			path: `/features/meter/properties/x/x1/x1.1/x1.1.1`,
		},

		// delete value from array
		{
			input: `{"foo": [{"foo": ["bar", "qux"], "": 0, "a/b": 1}, {"c%d": 2, "e^f": 3, "g|h": 4}, {"i\\j": 5, "k\"l": 6, " ": 7, "m~n": 8}]}`,
			command: `{
				 "topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				 %s,
				 "path": "/features/meter/properties/foo/2/m~n"
			 }`,
			output: `{"foo": [{"foo": ["bar", "qux"], "": 0, "a/b": 1}, {"c%d": 2, "e^f": 3, "g|h": 4}, {"i\\j": 5, "k\"l": 6, " ": 7}]}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/foo/2/m~n",
				"status": 204
			}`,
			path: `/features/meter/properties/foo/2/m~n`,
		},
	}

	featureIn := model.Feature{}
	featureOut := model.Feature{}

	for _, test := range tests {
		properties := asMapValue(s.T(), test.input)
		s.addFeature(testFeatureID, featureIn.WithProperties(properties))

		s.handleCommandF(test.command, defaultHeaders)

		s.getFeature(testFeatureID, &featureOut)
		assert.NotNil(s.T(), featureOut)

		properties = asMapValue(s.T(), test.output)
		assert.EqualValues(s.T(), properties, featureOut.Properties)
		event := s.asEnvelope(deletedEvent)
		if len(test.path) != 0 {
			event = event.WithPath(test.path)
		}
		assertPublished(s.S(), withHeadersNoResponseRequired(test.response), event)
	}
}

func (s *PropertyCommandsSuite) TestPropertyDeleteInvalid() {
	s.addTestThing()
	type propertyTest struct {
		input   string
		command string
	}
	tests := []propertyTest{

		// delete internal subjson with wrong path
		{
			input: `{"x": {"x1": {"x1.1": {"x1.1.1": {"x1.1.1.1": 11.1, "x1.2": 11.2}}, "x1.2": 11.2}}, "y": 20.0}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/x/x1/x1.1/x1.1.1.1"
			}`,
		},

		// delete from wrong index of array
		{
			input: `{"foo": {"inner": {"value1": 20, "array": [1, 2, 3]}}}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/foo/inner/array/10"
			}`,
		},

		// delete when there are no feature properties
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/properties/foo"
			}`,
		},
	}

	for _, test := range tests {
		assertFeatureNotModifiedWithCommand(s.S(), test.input, test.command, false)
	}
}

func (s *PropertyCommandsSuite) TestRetrievePropertyErrorStatus() {
	s.handleRetrieveCheckResponseF(retrieveXCmd, thingNotFoundErr)

	featuresIn := featuresAsMapValue(s.T(), "")
	s.addThing(featuresIn)
	s.handleRetrieveCheckResponseF(retrieveXCmd, featureNotFoundErr)

	featuresIn = featuresAsMapValue(s.T(), `{"meter": {}}`)
	s.addThing(featuresIn)
	s.handleRetrieveCheckResponseF(retrieveXCmd, propertyNotFoundErr)

	s.handleCommandF(retrieveXCmd, headersNoResponseRequired)
	assertPublishedNone(s.S())
}

func (s *PropertyCommandsSuite) TestDesiredPropertyModify() {
	s.addTestThing()
	type propertyTest struct {
		input       string
		command     string
		output      string
		response    string
		event       string
		retrieveRsp string
	}
	tests := []propertyTest{
		// add desired property
		{
			input: `{"x": 1.2}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties/y",
				"value": 3.4
			}`,
			output: `{"x": 1.2, "y": 3.4}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties/y",
				"status": 204
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/desiredProperties/y",
				"value": 3.4
			}`,
		},

		// modify existing desired property
		{
			input: `{"x": 1.2, "y": 3.4}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties/x",
				"value": 5.0
			}`,
			output: `{"x": 5.0, "y": 3.4}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties/x",
				"status": 204
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/desiredProperties/x",
				"value": 5.0
			}`,
			retrieveRsp: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
				%s,
				"path": "/features/meter/desiredProperties/x",
				"value":  5.0,
				"status": 200
			}`,
		},

		// create properties and add new
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties/x",
				"value": {
					"x1": 1.0,
					"x2": 2.0
				}
			}`,
			output: `{"x": {"x1": 1.0, "x2": 2.0}}`,
		},

		// add nonexisting path elements
		{
			input: `{"y": 3.4}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties/x/x1",
				"value": {
					"x1.1": 1.1,
					"x1.2": 1.2
				}
			}`,
			output: `{"x":{"x1": {"x1.1": 1.1, "x1.2": 1.2}}, "y": 3.4}`,
		},
	}

	featureIn := model.Feature{}
	featureOut := model.Feature{}

	for _, test := range tests {
		properties := asMapValue(s.T(), test.input)
		s.addFeature(testFeatureID, featureIn.WithDesiredProperties(properties))

		s.handleCommandF(test.command, defaultHeaders)
		if len(test.response) != 0 {
			assertPublishedOnOkF(s.S(), test.response, test.event)
		}

		s.getFeature(testFeatureID, &featureOut)
		assert.NotNil(s.T(), featureOut.DesiredProperties)

		properties = asMapValue(s.T(), test.output)
		assert.EqualValues(s.T(), properties, featureOut.DesiredProperties)

		s.handleRetrieveCheckResponseF(retrieveDesiredXCmd, test.retrieveRsp)
	}
}

func (s *PropertyCommandsSuite) TestDesiredPropertyDelete() {
	s.addTestThing()
	type propertyTest struct {
		input   string
		command string
		output  string
	}
	deleteDesiredXCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter/desiredProperties/x"
	}`
	tests := []propertyTest{
		// delete existing property on top level
		{
			input:   `{"x":{"x1": {"x1.1": 1.1, "x1.2": 1.2}}, "y": 3.0}`,
			command: deleteDesiredXCmd,
			output:  `{"y": 3.0}`,
		},

		// delete the only property
		{
			input:   `{"x":{"x1": 1.0, "x2": 2.0}}`,
			command: deleteDesiredXCmd,
		},

		// delete internal sub-json
		{
			input: `{"x": {"x1": {"x1.1": {"x1.1.1": {"x1.1.1.1": 11.1, "x1.2": 11.2}}, "x1.2": 11.2}}, "y": 20.0}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/desiredProperties/x/x1/x1.1/x1.1.1"
			}`,
			output: `{"x": {"x1": {"x1.1": {}, "x1.2": 11.2}}, "y": 20.0}`,
		},
	}

	featureIn := model.Feature{}
	featureOut := model.Feature{}

	for _, test := range tests {
		properties := asMapValue(s.T(), test.input)
		s.addFeature(testFeatureID, featureIn.WithDesiredProperties(properties))

		s.handleCommandF(test.command, defaultHeaders)

		s.getFeature(testFeatureID, &featureOut)
		properties = asMapValue(s.T(), test.output)
		assert.EqualValues(s.T(), properties, featureOut.DesiredProperties)
	}
}

func (s *PropertyCommandsSuite) TestDesiredPropertyDeleteInvalid() {
	s.addTestThing()
	type propertyTest struct {
		input   string
		command string
	}
	tests := []propertyTest{

		// delete internal sub-json with wrong path
		{
			input: `{"x": {"x1": {"x1.1": {"x1.1.1": {"x1.1.1.1": 11.1, "x1.2": 11.2}}, "x1.2": 11.2}}, "y": 20.0}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/desiredProperties/x/x1/x1.1/x1.1.1.1"
			}`,
		},

		// delete from wrong index of array
		{
			input: `{"foo": {"inner": {"value1": 20, "array": [1, 2, 3]}}}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/desiredProperties/foo/inner/array/10"
			}`,
		},

		// delete when there are no feature's desired properties
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/desiredProperties/foo"
			}`,
		},
	}

	for _, test := range tests {
		assertFeatureNotModifiedWithCommand(s.S(), test.input, test.command, true)
	}
}

func (s *PropertyCommandsSuite) TestRetrieveDesiredPropertyErrorStatus() {
	s.handleRetrieveCheckResponseF(retrieveDesiredXCmd, thingNotFoundErr)

	featuresIn := featuresAsMapValue(s.T(), "")
	s.addThing(featuresIn)
	s.handleRetrieveCheckResponseF(retrieveDesiredXCmd, featureNotFoundErr)

	featuresIn = featuresAsMapValue(s.T(), `{"meter": {}}`)
	s.addThing(featuresIn)
	s.retrieveDesiredPropertyNotFoundError()
}

func (s *PropertyCommandsSuite) retrieveDesiredPropertyNotFoundError() {
	retrieveRsp := `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 404,
			"error": "things:feature.desiredProperty.notfound",
			"message": "The desired property with JSON Pointer '/x' of the Feature with ID 'meter' on the Thing with ID 'org.eclipse.kanto:test' does not exist.",
			"description": "Check if the ID of the Thing, the Feature ID and the key of your requested property was correct."
		},
		"status": 404
	}`
	s.handleRetrieveCheckResponseF(retrieveDesiredXCmd, retrieveRsp)
}
