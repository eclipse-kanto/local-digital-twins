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

	"github.com/eclipse/ditto-clients-golang/protocol/things"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ldtFeatureSuite struct {
	localDigitalTwinsSuite

	messagesFilter string
	expectedPath   string
}

func (suite *ldtFeatureSuite) SetupSuite() {
	suite.SetupLdtSuite()
	suite.messagesFilter = "like(resource:path,'/features/*')"
	suite.expectedPath = fmt.Sprintf("/features/%s", featureID)

}

func (suite *ldtFeatureSuite) TearDownSuite() {
	suite.TearDownLdtSuite()
	suite.TearDown()
}

func TestFeatureSuite(t *testing.T) {
	suite.Run(t, new(ldtFeatureSuite))
}

func (suite *ldtFeatureSuite) TestEventModifyOrCreateFeature() {
	tests := map[string]ldtTestCaseData{
		"test_create_feature": {
			command:       things.NewCommand(suite.namespacedID).Twin().Feature(featureID).Modify(emptyFeature),
			expectedTopic: suite.twinEventTopicCreated,
		},

		"test_modify_feature": {
			command:       things.NewCommand(suite.namespacedID).Twin().Feature(featureID).Modify(emptyFeature),
			expectedTopic: suite.twinEventTopicModified,
			feature:       emptyFeature,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.feature != nil {
				suite.createTestFeature(testCase.feature, featureID)
			}
			suite.executeCommandEvent("e", suite.messagesFilter, emptyFeature, testCase.command, suite.expectedPath, testCase.expectedTopic)
			expectedBody, _ := json.Marshal(emptyFeature)
			actualBody, err := suite.getFeature(featureID)
			require.NoError(suite.T(), err, "unable to get feature")

			assert.True(suite.T(), reflect.DeepEqual(suite.convertToMap(expectedBody), suite.convertToMap(actualBody)))
			suite.removeTestFeatures()
		})
	}
}

func (suite *ldtFeatureSuite) TestEventDeleteFeature() {
	suite.createTestFeature(emptyFeature, featureID)
	suite.executeCommandEvent("e", suite.messagesFilter, nil, things.NewCommand(suite.namespacedID).Twin().Feature(featureID).Delete(), suite.expectedPath, suite.twinEventTopicDeleted)

	body, err := suite.getFeature(featureID)
	require.Error(suite.T(), err, "feature should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")
}

func (suite *ldtFeatureSuite) TestCommandResponseModifyOrCreateFeature() {
	tests := map[string]ldtTestCaseData{
		"test_create_feature": {
			command:            things.NewCommand(suite.namespacedID).Twin().Feature(featureID).Modify(emptyFeature),
			expectedStatusCode: 201,
		},

		"test_modify_feature": {
			command:            things.NewCommand(suite.namespacedID).Twin().Feature(featureID).Modify(emptyFeature),
			expectedStatusCode: 204,
			feature:            emptyFeature,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.feature != nil {
				suite.createTestFeature(testCase.feature, featureID)
			}
			response, err := suite.executeCommandResponse(testCase.command)
			require.NoError(suite.T(), err, "could not get response")
			assert.Equal(suite.T(), testCase.expectedStatusCode, response.Status, "unexpected status code")
			suite.removeTestFeatures()
		})
	}
}

func (suite *ldtFeatureSuite) TestCommandResponseDeleteFeature() {
	suite.createTestFeature(emptyFeature, featureID)
	response, err := suite.executeCommandResponse(things.NewCommand(suite.namespacedID).Feature(featureID).Delete())
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 204, response.Status, "unexpected status code")
}

func (suite *ldtFeatureSuite) TestCommandResponseRetrieveFeature() {
	suite.createTestFeature(emptyFeature, featureID)
	response, err := suite.executeCommandResponse(things.NewCommand(suite.namespacedID).Feature(featureID).Retrieve())
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 200, response.Status, "unexpected status code")
	actualBody, err := suite.getFeature(featureID)
	require.NoError(suite.T(), err, "unable to get feature")
	assert.True(suite.T(), reflect.DeepEqual(response.Value, suite.convertToMap(actualBody)))
}
