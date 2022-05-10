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
	deletePropertiesCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter/properties"
	}`
	deleteDesiredPropertiesCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter/desiredProperties"
	}`

	retrievePropertiesCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/features/meter/properties"
	}`
	retrieveDesiredPropertiesCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/features/meter/desiredProperties"
	}`

	desiredPropertiesNotFoundErr = `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 404,
			"error": "things:feature.desiredProperties.notfound",
			"message": "The desired properties of the Feature with ID 'meter' on the Thing with ID 'org.eclipse.kanto:test' do not exist.",
			"description": "Check if the ID of the Thing and the Feature ID was correct."
		},
		"status": 404
	}`
)

type PropertiesCommandsSuite struct {
	CommandsSuite
}

func TestPropertiesCommandsSuite(t *testing.T) {
	suite.Run(t, new(PropertiesCommandsSuite))
}

func (s *PropertiesCommandsSuite) TestPropertiesModify() {
	s.addTestThing()
	type proprtiesTest struct {
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
		"path": "/features/meter/properties",
		"status": 204
	}`

	tests := []proprtiesTest{
		// create properties and add new
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x1": 1.0,
					"x2": 2.0
				}
			}`,
			output: `{"x1": 1.0, "x2": 2.0}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"status": 201
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/created",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x1": 1.0,
					"x2": 2.0
				}
			}`,
		},

		// add property
		{
			input: `{"x": 1.2}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": 12.34,
					"y": 5.6
				}
			}`,
			output:   `{"x": 12.34, "y": 5.6}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": 12.34,
					"y": 5.6
				}
			}`,
			retrieveRsp: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": 12.34,
					"y": 5.6
				},
				"status": 200
			}`,
		},

		// modify existing property
		{
			input: `{"x": 1.2, "y": 3.4}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": 5.0,
					"y": {
						"y1": 6.0,
						"y2": "test2"
					}
				}
			}`,
			output:   `{"x": 5.0, "y": {"y1": 6.0, "y2": "test2"}}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": 5.0,
					"y": {
						"y1": 6.0,
						"y2": "test2"
					}
				}
			}`,
		},

		// modify from numeric to object
		{
			input: `{"x": 1.2}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": {
						"x1": 1.0,
						"x2": 2.0
					}
				}
			}`,
			output:   `{"x":{"x1": 1.0, "x2": 2.0}}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": {
						"x1": 1.0,
						"x2": 2.0
					}
				}
			}`,
		},

		// add unexisting path elements
		{
			input: `{"y": 3.4}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": {
						"x1": {
							"x1.1": 1.1,
							"x1.2": 1.2
						}
					},
					"y": 5.6
				}
			}`,
			output:   `{"x":{"x1": {"x1.1": 1.1, "x1.2": 1.2}}, "y": 5.6}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": {
						"x1": {
							"x1.1": 1.1,
							"x1.2": 1.2
						}
					},
					"y": 5.6
				}
			}`,
		},

		// modify from numeric to object
		{
			input: `{"x": 0.0, "y": 3.0}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": {
						"x1": 1.0,
						"x2": 2.0
					},
					"y": 3.3
				}
			}`,
			output:   `{"x": {"x1": 1.0, "x2": 2.0}, "y": 3.3}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": {
						"x1": 1.0,
						"x2": 2.0
					},
					"y": 3.3
				}
			}`,
		},

		// modify existing with creation of nested unexisting path elements
		{
			input: `{"x": 10.0, "y": 20.0}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": {
						"x1": {
							"x1.1": 11.1,
							"x1.2": 11.2
						}
					},
					"y": "test",
					"z": 30.0
				}
			}`,
			output:   `{"x":{"x1": {"x1.1": 11.1, "x1.2": 11.2}}, "y": "test", "z": 30.0}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties",
				"value": {
					"x": {
						"x1": {
							"x1.1": 11.1,
							"x1.2": 11.2
						}
					},
					"y": "test",
					"z": 30.0
				}
			}`,
		},

		// modify existing with empty
		{
			input: `{"x":{"x1": {"x1.1": 11.1, "x1.2": 11.2}}, "y": "test", "z": 30.0}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": {}
			}`,
			output:   `{}`,
			response: expResponse,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/modified",
				%s,
				"path": "/features/meter/properties",
				"value": {}
			}`,
			retrieveRsp: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
				%s,
				"path": "/features/meter/properties",
				"value": {},
				"status": 200
			}`,
		},
	}

	featureIn := model.Feature{}
	featureOut := model.Feature{}

	for _, test := range tests {
		properties := asMapValue(s.T(), test.input)
		s.addFeature(testFeatureID, featureIn.WithProperties(properties))

		s.handleCommandF(test.command, defaultHeaders)

		s.getFeature(testFeatureID, &featureOut)
		assert.NotNil(s.T(), featureOut.Properties)

		properties = asMapValue(s.T(), test.output)
		assert.EqualValues(s.T(), properties, featureOut.Properties)

		assertPublishedOnOkF(s.S(), test.response, test.event)

		s.handleRetrieveCheckResponseF(retrievePropertiesCmd, test.retrieveRsp)
	}
}

func (s *PropertiesCommandsSuite) TestPropertiesDelete() {
	s.addTestThing()
	type proprtiesTest struct {
		input string
	}
	tests := []proprtiesTest{
		// deleting all existing properties
		{
			input: `{"x":{"x1": {"x1.1": 11.1, "x1.2": 11.2}}, "y": "test", "z": 30.0}`,
		},

		// deleting non existing properties
		{},
	}

	featureIn := model.Feature{}
	featureOut := model.Feature{}

	for _, test := range tests {
		properties := asMapValue(s.T(), test.input)
		s.addFeature(testFeatureID, featureIn.WithProperties(properties))

		s.handleCommandF(deletePropertiesCmd, defaultHeaders)

		s.getFeature(testFeatureID, &featureOut)
		assert.Nil(s.T(), featureOut.Properties)
	}
}

func (s *PropertiesCommandsSuite) TestRetrievePropertiesErrorStatus() {
	s.handleRetrieveCheckResponseF(retrievePropertiesCmd, thingNotFoundErr)

	featuresIn := featuresAsMapValue(s.T(), "")
	s.addThing(featuresIn)
	s.handleRetrieveCheckResponseF(retrievePropertiesCmd, featureNotFoundErr)

	featuresIn = featuresAsMapValue(s.T(), `{"meter": {}}`)
	s.addThing(featuresIn)
	s.retrievePropertiesNotFoundError()

	s.handleCommandF(retrievePropertiesCmd, headersNoResponseRequired)
	assertPublishedNone(s.S())
}

func (s *PropertiesCommandsSuite) TestDesiredPropertiesModify() {
	s.addTestThing()
	type propertiesTest struct {
		input   string
		command string
		output  string
	}
	tests := []propertiesTest{
		// add desired property
		{
			input: `{"x": 1.2}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties",
				"value": {
					"x": 12.34,
					"y": 5.6
				  }
			}`,
			output: `{"x": 12.34, "y": 5.6}`,
		},

		// modify existing desired property
		{
			input: `{"x": 1.2, "y": 3.4}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties",
				"value": {
					"x": 5.0,
					"y": {
						"y1": 6.0,
						"y2": "test2"
					}
				}
			}`,
			output: `{"x": 5.0, "y": {"y1": 6.0, "y2": "test2"}}`,
		},

		// create desired properties
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties",
				"value": {
					"x1": 1.0,
					"x2": 2.0
				}
			}`,
			output: `{"x1": 1.0, "x2": 2.0}`,
		},

		// add unexisting desired path element
		{
			input: `{"y": 3.4}`,
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/desiredProperties",
				"value": {
					"x": {
						"x1": {
							"x1.1": 1.1,
							"x1.2": 1.2
						}
					},
					"y": 5.6
				}
			}`,
			output: `{"x":{"x1": {"x1.1": 1.1, "x1.2": 1.2}}, "y": 5.6}`,
		},
	}

	featureIn := model.Feature{}
	featureOut := model.Feature{}

	for _, test := range tests {
		properties := asMapValue(s.T(), test.input)
		s.addFeature(testFeatureID, featureIn.WithDesiredProperties(properties))

		s.handleCommandF(test.command, defaultHeaders)

		s.getFeature(testFeatureID, &featureOut)
		assert.NotNil(s.T(), featureOut.DesiredProperties)

		properties = asMapValue(s.T(), test.output)
		assert.EqualValues(s.T(), properties, featureOut.DesiredProperties)
	}
}

func (s *PropertiesCommandsSuite) TestDesiredPropertiesDelete() {
	s.addTestThing()
	type proprtiesTest struct {
		input    string
		response string
		event    string
	}
	tests := []proprtiesTest{
		// deleting all existing desired properties
		{
			input: `{"x":{"x1": {"x1.1": 11.1, "x1.2": 11.2}}, "y": "test", "z": 30.0}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "/features/meter/desiredProperties",
				"status": 204
			}`,
			event: `{
				"topic": "org.eclipse.kanto/test/things/twin/events/deleted",
				%s,
				"path": "/features/meter/desiredProperties"
			}`,
		},

		// deleting non existing desired properties
		{
			response: desiredPropertiesNotFoundErr,
		},
	}

	featureIn := model.Feature{}
	featureOut := model.Feature{}

	for _, test := range tests {
		properties := asMapValue(s.T(), test.input)
		s.addFeature(testFeatureID, featureIn.WithDesiredProperties(properties))

		s.handleCommandF(deleteDesiredPropertiesCmd, defaultHeaders)

		s.getFeature(testFeatureID, &featureOut)
		assert.Nil(s.T(), featureOut.DesiredProperties)

		if len(test.event) == 0 {
			assertPublishedOnErrorF(s.S(), test.response)
		} else {
			assertPublishedOnDeletedF(s.S(), test.response, test.event)
		}
	}
}

func (s *PropertiesCommandsSuite) TestRetrieveDesiredPropertiesErrorStatus() {
	s.handleRetrieveCheckResponseF(retrieveDesiredPropertiesCmd, thingNotFoundErr)

	featuresIn := featuresAsMapValue(s.T(), "")
	s.addThing(featuresIn)
	s.handleRetrieveCheckResponseF(retrieveDesiredPropertiesCmd, featureNotFoundErr)

	featuresIn = featuresAsMapValue(s.T(), `{"meter": {}}`)
	s.addThing(featuresIn)
	s.handleRetrieveCheckResponseF(retrieveDesiredPropertiesCmd, desiredPropertiesNotFoundErr)
}

func (s *PropertiesCommandsSuite) retrievePropertiesNotFoundError() {
	retrieveRsp := `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 404,
			"error": "things:feature.properties.notfound",
			"message": "The Properties of the Feature with ID 'meter' on the Thing with ID 'org.eclipse.kanto:test' do not exist.",
			"description": "Check if the ID of the Thing and the Feature ID was correct."
		},
		"status": 404
	}`
	s.handleRetrieveCheckResponseF(retrievePropertiesCmd, retrieveRsp)
}
