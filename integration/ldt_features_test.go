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
	"reflect"
	"testing"

	"github.com/eclipse/ditto-clients-golang/model"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ldtFeaturesSuite struct {
	localDigitalTwinsSuite

	messagesFilter string
	expectedPath   string
}

func (suite *ldtFeaturesSuite) SetupSuite() {
	suite.SetupLdtSuite()
	suite.messagesFilter = "like(resource:path,'/features')"
	suite.expectedPath = "/features"
}

func (suite *ldtFeaturesSuite) TearDownSuite() {
	suite.TearDownLdtSuite()
	suite.TearDown()
}

func TestFeaturesSuite(t *testing.T) {
	suite.Run(t, new(ldtFeaturesSuite))
}

func (suite *ldtFeaturesSuite) TestEventModifyOrCreateFeatures() {
	tests := map[string]ldtTestCaseData{
		"test_create_features": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Features().Modify(features),
			expectedTopic: suite.twinEventTopicCreated,
		},

		"test_modify_features": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Features().Modify(features),
			expectedTopic: suite.twinEventTopicModified,
			feature:       emptyFeature,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.feature != nil {
				suite.createTestFeature(testCase.feature, featureID)
			}
			suite.executeCommandEvent("e", suite.messagesFilter, features, testCase.command, suite.expectedPath, testCase.expectedTopic)
			expectedBody, _ := json.Marshal(features)
			actualBody, err := suite.getAllFeatures()
			require.NoError(suite.T(), err, "unable to get features")

			assert.True(suite.T(), reflect.DeepEqual(suite.convertToMap(expectedBody), suite.convertToMap(actualBody)))
			suite.removeTestFeatures()
		})
	}
}
func (suite *ldtFeaturesSuite) TestEventDeleteFeatures() {
	suite.createTestFeature(emptyFeature, featureID)
	suite.executeCommandEvent("e", suite.messagesFilter, nil, things.NewCommand(suite.namespacedID).Twin().Features().Delete(), suite.expectedPath, suite.twinEventTopicDeleted)

	body, err := suite.getAllFeatures()
	require.Error(suite.T(), err, "features should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")
}

func (suite *ldtFeaturesSuite) TestCommandResponseModifyOrCreateFeatures() {
	tests := map[string]ldtTestCaseData{
		"test_create_features": {
			command:            things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Features().Modify(features),
			expectedStatusCode: 201,
		},

		"test_modify_features": {
			command:            things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Features().Modify(features),
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

func (suite *ldtFeaturesSuite) TestCommandResponseDeleteFeatures() {
	suite.createTestFeature(emptyFeature, featureID)
	response, err := suite.executeCommandResponse(things.NewCommand(suite.namespacedID).Features().Delete())
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 204, response.Status, "unexpected status code")
}

func (suite *ldtFeaturesSuite) TestCommandResponseRetrieveFeatures() {
	suite.createTestFeature(emptyFeature, featureID)
	response, err := suite.executeCommandResponse(things.NewCommand(suite.namespacedID).Features().Retrieve())
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 200, response.Status, "unexpected status code")

	actualBody, err := suite.getAllFeatures()
	require.NoError(suite.T(), err, "unable to get features")
	assert.True(suite.T(), reflect.DeepEqual(response.Value, suite.convertToMap(actualBody)))
}
