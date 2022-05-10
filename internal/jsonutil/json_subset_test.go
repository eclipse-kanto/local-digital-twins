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

package jsonutil_test

import (
	"fmt"
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/jsonutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testThing = `{
		"thingId": "org.eclipse.kanto:test:thing1",
		"policyId": "org.eclipse.kanto:test:thing1",
		"attributes": {
			"Info": {
				"gatewayId": "org.eclipse.kanto:test"
			},
			"test.attribute": "test.Attribute"
		},
		"features": {
			"meter": {
				"properties": {
					"attributes": {
						"factory.uid": "test:Factory"
					},
					"configuration": {
						"name": "Factory Created Item Demo",
						"tags": {"tag1":"value1","tag2":"value2"}
					},
					"status": {
						"state": "OFF"
					}
				},
				"desiredProperties": {
					"configuration": {
						"name": "Factory Created Desired Demo",
						"tags": "[test_Tag, tag]"
					},
					"status": {
						"state": "ON"
					}
				}
			},
			"feature:test": {
				"definition": [
					"org.eclipse.kantot.test:services.TestItem:1.1.0"
				],
				"properties": {
					"configuration": {
						"symbol": "A",
						"complex": null,
						"name": "Supported Types Demo",
						"caption": "Demo Binary Switch",
						"tags": []
					},
					"attributes": {
						"label": "demo"
					},
					"status": {
						"myProperty": 0,
						"state": "OFF",
						"open": false
					}
				}
			},
			"feature:bs": {
				"definition": [
					"org.eclipse.kantot.test:BinarySwitch:1.0.0",
					"org.eclipse.kantot.test:services.TestItem:1.1.0"
				],
				"properties": {
					"attributes": {
						"factory.uid": "demo:Factory"
					},
					"configuration": {
						"name": "Factory Created Item Demo",
						"tags": null
					},
					"status": {
						"state": "OFF"
					}
				}
			}
		}
	}`
)

func TestJSONSubsetSimpleFilter(t *testing.T) {
	filter := "thingId,features/meter/properties/configuration/name,attributes/Info"

	string, err := jsonutil.JSONSubset(testThing, filter)
	require.NoError(t, err)

	expected := `{
		"thingId":"org.eclipse.kanto:test:thing1",
		"features":{
			"meter":{
				"properties":{
					"configuration":{
						"name":"Factory Created Item Demo"
					}
				}
			}
		},
		"attributes":{"Info":{"gatewayId":"org.eclipse.kanto:test"}}
	}`
	assert.JSONEq(t, expected, string)
}

func TestJSONSubsetDotKeyFilter(t *testing.T) {
	string, err := jsonutil.JSONSubset(testThing, "thingId,attributes/test.attribute")
	require.NoError(t, err)

	expected := `{
		"thingId":"org.eclipse.kanto:test:thing1",
		"attributes":
			{
				"test.attribute":"test.Attribute"
			}
	}`
	assert.JSONEq(t, expected, string)
}

func TestJSONSubset(t *testing.T) {
	filter := "thingId,features(meter,meter/properties/configuration/name,meter/properties/configuration/tags,feature:test),attributes(Info,foo/test)"

	subset, err := jsonutil.JSONSubset(testThing, filter)
	require.NoError(t, err)

	expected := `{
		"thingId":"org.eclipse.kanto:test:thing1",
		"attributes":{"Info":{"gatewayId":"org.eclipse.kanto:test"}},
		"features":{
			"feature:test":{
				"definition":["org.eclipse.kantot.test:services.TestItem:1.1.0"],
				"properties":{
					"attributes":{"label":"demo"},
					"configuration":{
						"caption":"Demo Binary Switch",
						"complex":null,
						"name":"Supported Types Demo",
						"symbol":"A",
						"tags":[]
					},
					"status":{
						"myProperty":0,
						"open":false,
						"state":"OFF"
					}
				}
			},
			"meter":{
				"properties":{
					"configuration":{
						"name":"Factory Created Item Demo",
						"tags":{
							"tag1":"value1",
							"tag2":"value2"
						}
					}
				}
			}
		}
	}`
	assert.JSONEq(t, expected, subset)
}

func TestJSONSubsetInvalid(t *testing.T) {
	arrSubset, err := jsonutil.JSONSubset("{invalid}", "thingId")
	assert.Error(t, err)
	assert.Empty(t, arrSubset)
}

func TestJSONSubsetArray(t *testing.T) {
	testMulti := `[
		%s,
		{
			"thingId": "org.eclipse.kanto:test:thing2",
			"definition": "org.eclipse.kanto:Sensor:1.0.0",
			"attributes": {
				"foo.key": {
					"gatewayId": "org.eclipse.kanto:test"
				},
				"bar": {
					"baz": "Test Baz",
					"test": "TEST test"
				}
			},
			"features": {
				"feature:1": {
					"definition": [
						"org.eclipse.kantot.test:Switch:1.0.0",
						"org.eclipse.kantot.test:TestItem:1.1.0"
					],
					"properties": {
						"name": "Factory Created Item Demo"
					}
				},
				"myTest": {
					"properties": {
						"prop1": "prop1Value"
					}
				}
			}
		}
	]`

	filter := "thingId,features(meter,meter/properties,feature:1),attributes(Info,foo.key,bar/baz)"

	thingArr := fmt.Sprintf(testMulti, testThing)
	arrSubset, err := jsonutil.JSONArraySubset(thingArr, filter)
	require.NoError(t, err)

	expected := `[
		{
			"attributes":{
				"Info":{
					"gatewayId":"org.eclipse.kanto:test"
				}
			},
			"features":{
				"meter":{
					"properties":{
						"attributes":{
							"factory.uid":"test:Factory"
						},
						"configuration":{
							"name":"Factory Created Item Demo",
							"tags":{"tag1":"value1","tag2":"value2"}
						},
						"status":{
							"state":"OFF"
						}
					}
				}
			},
			"thingId":"org.eclipse.kanto:test:thing1"
		},
		{
			"attributes":{
				"bar":{"baz":"Test Baz"},
				"foo.key":{"gatewayId":"org.eclipse.kanto:test"}
			},
			"features":{
				"feature:1":{
					"definition":[
						"org.eclipse.kantot.test:Switch:1.0.0",
						"org.eclipse.kantot.test:TestItem:1.1.0"],
					"properties":{
						"name":"Factory Created Item Demo"
					}
				}
			},
			"thingId":"org.eclipse.kanto:test:thing2"
		}
]`

	assert.JSONEq(t, expected, arrSubset)
}

func TestJSONArraySubsetNonarray(t *testing.T) {
	filter := "thingId,features(meter,meter/properties,feature:1),attributes(Info,foo.key,bar/baz)"
	arrSubset, err := jsonutil.JSONArraySubset(testThing, filter)
	assert.Error(t, err)
	assert.Empty(t, arrSubset)
}

func TestJSONArraySubsetInvalidFilter(t *testing.T) {
	arrSubset, err := jsonutil.JSONArraySubset(fmt.Sprintf("[%s]", testThing), "test(invalid")
	assert.Error(t, err)
	assert.Empty(t, arrSubset)
}
