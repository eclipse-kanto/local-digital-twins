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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"

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

			expectedMap := suite.convertToMap(expectedBody)
			actualMap := suite.convertToMap(actualBody)
			assert.True(suite.T(), reflect.DeepEqual(expectedMap, actualMap))
			suite.removeTestFeatures()
		})
	}
}
func (suite *ldtPropertiesSuite) TestEventDeleteProperties() {
	command := things.NewCommand(suite.namespacedID).Twin().FeatureProperties(featureID).Delete()
	expectedTopic := suite.twinEventTopicDeleted
	suite.createTestFeature(featureWithProperties, featureID)
	suite.executeCommandEvent("e", suite.messagesFilter, nil, command, suite.expectedPath, expectedTopic)
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
	command := things.NewCommand(suite.namespacedID).FeatureProperties(featureID).Delete()
	suite.createTestFeature(featureWithProperties, featureID)
	response, err := suite.executeCommandResponse(command)
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 204, response.Status, "unexpected status code")
}

func (suite *ldtPropertiesSuite) TestCommandResponseRetrieveProperties() {
	command := things.NewCommand(suite.namespacedID).FeatureProperties(featureID).Retrieve()
	suite.createTestFeature(featureWithProperties, featureID)
	response, err := suite.executeCommandResponse(command)
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 200, response.Status, "unexpected status code")
	actualBody, _ := suite.getAllPropertiesOfFeature(featureID)
	require.NoError(suite.T(), err, "unable to get properties")
	actualMap := suite.convertToMap(actualBody)
	assert.True(suite.T(), reflect.DeepEqual(response.Value, actualMap))
}
