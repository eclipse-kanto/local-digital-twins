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

type ldtDesiredPropertiesSuite struct {
	localDigitalTwinsSuite

	messagesFilter string
	expectedPath   string
}

func (suite *ldtDesiredPropertiesSuite) SetupSuite() {
	suite.SetupLdtSuite()
	suite.messagesFilter = fmt.Sprintf("like(resource:path,'/features/%s/desiredProperties')", featureID)
	suite.expectedPath = fmt.Sprintf("/features/%s/desiredProperties", featureID)
}

func (suite *ldtDesiredPropertiesSuite) TearDownSuite() {
	suite.TearDownLdtSuite()
	suite.TearDown()
}

func TestDesiredPropertiesSuite(t *testing.T) {
	suite.Run(t, new(ldtDesiredPropertiesSuite))
}

func (suite *ldtDesiredPropertiesSuite) TestEventModifyOrCreateDesiredProperties() {
	tests := map[string]ldtTestCaseData{
		"test_create_desired_properties": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().FeatureDesiredProperties(featureID).Modify(properties),
			expectedTopic: suite.twinEventTopicCreated,
			feature:       emptyFeature,
		},

		"test_modify_desired_properties": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				FeatureDesiredProperties(featureID).Modify(properties),
			expectedTopic: suite.twinEventTopicModified,
			feature:       featureWithDesiredProperties,
		},
	}

	for testName, testCase := range tests {
		suite.Run(testName, func() {
			suite.createTestFeature(testCase.feature, featureID)
			suite.executeCommandEvent("e", suite.messagesFilter, properties, testCase.command, suite.expectedPath, testCase.expectedTopic)
			expectedBody, _ := json.Marshal(properties)
			actualBody, err := suite.getAllDesiredPropertiesOfFeature(featureID)
			require.NoError(suite.T(), err, "unable to get desired properties")

			expectedMap := suite.convertToMap(expectedBody)
			actualMap := suite.convertToMap(actualBody)
			assert.True(suite.T(), reflect.DeepEqual(expectedMap, actualMap))
			suite.removeTestFeatures()
		})
	}
}

func (suite *ldtDesiredPropertiesSuite) TestEventDeleteDesiredProperties() {
	command := things.NewCommand(suite.namespacedID).FeatureDesiredProperties(featureID).Delete()
	expectedTopic := suite.twinEventTopicDeleted

	suite.createTestFeature(featureWithDesiredProperties, featureID)
	suite.executeCommandEvent("e", suite.messagesFilter, nil, command, suite.expectedPath, expectedTopic)

	body, err := suite.getAllDesiredPropertiesOfFeature(featureID)
	require.Error(suite.T(), err, "desired properties of feature should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")
}

func (suite *ldtDesiredPropertiesSuite) TestCommandResponseModifyOrCreateDesiredProperties() {
	tests := map[string]ldtTestCaseData{
		"test_create_desired_properties": {
			command:            things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().FeatureDesiredProperties(featureID).Modify(properties),
			expectedStatusCode: 201,
			feature:            emptyFeature,
		},

		"test_modify_desired_properties": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				FeatureDesiredProperties(featureID).Modify(properties),
			expectedStatusCode: 204,
			feature:            featureWithDesiredProperties,
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

func (suite *ldtDesiredPropertiesSuite) TestCommandResponseDeleteDesiredProperties() {
	command := things.NewCommand(suite.namespacedID).FeatureDesiredProperties(featureID).Delete()
	suite.createTestFeature(featureWithDesiredProperties, featureID)
	response, err := suite.executeCommandResponse(command)
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 204, response.Status, "unexpected status code")
}

func (suite *ldtDesiredPropertiesSuite) TestCommandResponseRetrieveDesiredProperties() {
	command := things.NewCommand(suite.namespacedID).FeatureDesiredProperties(featureID).Retrieve()
	suite.createTestFeature(featureWithDesiredProperties, featureID)
	response, err := suite.executeCommandResponse(command)
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 200, response.Status, "unexpected status code")
	actualBody, _ := suite.getAllDesiredPropertiesOfFeature(featureID)
	require.NoError(suite.T(), err, "unable to get desired properties")
	actualMap := suite.convertToMap(actualBody)
	assert.True(suite.T(), reflect.DeepEqual(response.Value, actualMap))
}
