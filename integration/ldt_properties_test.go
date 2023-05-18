// Copyright (c) 2023 Contributors to the Eclipse Foundation
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

//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eclipse/ditto-clients-golang/model"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
	"github.com/stretchr/testify/suite"
)

type ldtPropertiesSuite struct {
	localDigitalTwinsSuite

	messagesFilter string
	expectedPath   string
}

func (suite *ldtPropertiesSuite) SetupSuite() {
	suite.SetupLdtSuite()
	suite.messagesFilter = fmt.Sprintf("like(resource:path,'/features/%s/properties')", featureID)
	suite.expectedPath = fmt.Sprintf("/features/%s/properties", featureID)
}

func (suite *ldtPropertiesSuite) TearDownSuite() {
	suite.TearDownLdtSuite()
	suite.TearDown()
}

func TestPropertiesSuite(t *testing.T) {
	suite.Run(t, new(ldtPropertiesSuite))
}

func (suite *ldtPropertiesSuite) TestEventModifyOrCreateProperties() {
	tests := map[string]ldtTestCaseData{
		"test_create_properties": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().FeatureProperties(featureID).Modify(properties),
			expectedTopic: suite.twinEventTopicCreated,
			feature:       emptyFeature,
		},

		"test_modify_properties": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				FeatureProperties(featureID).Modify(properties),
			expectedTopic: suite.twinEventTopicModified,
			feature:       featureWithProperties,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			suite.createTestFeature(testCase.feature, featureID)
			suite.executeCommandEvent("e", suite.messagesFilter, properties, testCase.command, suite.expectedPath, testCase.expectedTopic)
			expectedBody, _ := json.Marshal(properties)
			actualBody, err := suite.getAllPropertiesOfFeature(featureID)
			require.NoError(suite.T(), err, "unable to get properties")

			assert.True(suite.T(), reflect.DeepEqual(suite.convertToMap(expectedBody), suite.convertToMap(actualBody)))
			suite.removeTestFeatures()
		})
	}
}
func (suite *ldtPropertiesSuite) TestEventDeleteProperties() {
	suite.createTestFeature(featureWithProperties, featureID)
	suite.executeCommandEvent("e", suite.messagesFilter, nil, things.NewCommand(suite.namespacedID).Twin().FeatureProperties(featureID).Delete(), suite.expectedPath, suite.twinEventTopicDeleted)
	body, err := suite.getAllPropertiesOfFeature(featureID)
	require.Error(suite.T(), err, "properties should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")
}

func (suite *ldtPropertiesSuite) TestCommandResponseModifyOrCreateProperties() {
	tests := map[string]ldtTestCaseData{
		"test_create_properties": {
			command:            things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().FeatureProperties(featureID).Modify(properties),
			expectedStatusCode: 201,
			feature:            emptyFeature,
		},

		"test_modify_properties": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				FeatureProperties(featureID).Modify(properties),
			expectedStatusCode: 204,
			feature:            featureWithProperties,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			suite.createTestFeature(testCase.feature, featureID)
			response, err := suite.executeCommandResponse(testCase.command)
			require.NoError(suite.T(), err, "could not get response")
			assert.Equal(suite.T(), testCase.expectedStatusCode, response.Status, "unexpected status code")
			suite.removeTestFeatures()
		})
	}
}

func (suite *ldtPropertiesSuite) TestCommandResponseDeleteProperties() {
	suite.createTestFeature(featureWithProperties, featureID)
	response, err := suite.executeCommandResponse(things.NewCommand(suite.namespacedID).FeatureProperties(featureID).Delete())
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 204, response.Status, "unexpected status code")
}

func (suite *ldtPropertiesSuite) TestCommandResponseRetrieveProperties() {
	suite.createTestFeature(featureWithProperties, featureID)
	response, err := suite.executeCommandResponse(things.NewCommand(suite.namespacedID).FeatureProperties(featureID).Retrieve())
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 200, response.Status, "unexpected status code")
	actualBody, err := suite.getAllPropertiesOfFeature(featureID)
	require.NoError(suite.T(), err, "unable to get properties")
	assert.True(suite.T(), reflect.DeepEqual(response.Value, suite.convertToMap(actualBody)))
}
