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
	"strings"
	"testing"

	"github.com/eclipse/ditto-clients-golang/model"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
	"github.com/stretchr/testify/suite"
)

type ldtDesiredPropertySuite struct {
	localDigitalTwinsSuite

	messagesFilter string
	expectedPath   string
}

func (suite *ldtDesiredPropertySuite) SetupSuite() {
	suite.SetupLdtSuite()
	suite.messagesFilter = fmt.Sprintf("like(resource:path,'/features/%s/desiredProperties/*')", featureID)
	suite.expectedPath = fmt.Sprintf("/features/%s/desiredProperties/%s", featureID, desiredProperty)
}

func (suite *ldtDesiredPropertySuite) TearDownSuite() {
	suite.TearDownLdtSuite()
	suite.TearDown()
}

func TestDesiredPropertySuite(t *testing.T) {
	suite.Run(t, new(ldtDesiredPropertySuite))
}

func (suite *ldtDesiredPropertySuite) TestEventModifyOrCreateProperty() {
	tests := map[string]ldtTestCaseData{
		"test_create_desired_property": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().FeatureDesiredProperty(featureID, desiredProperty).Modify(value),
			expectedTopic: suite.twinEventTopicCreated,
			feature:       emptyFeature,
		},

		"test_modify_desired_property": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				FeatureDesiredProperty(featureID, desiredProperty).Modify(value),
			expectedTopic: suite.twinEventTopicModified,
			feature:       featureWithDesiredProperties,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			suite.createTestFeature(testCase.feature, featureID)
			suite.executeCommandEvent("e", suite.messagesFilter, value, testCase.command, suite.expectedPath, testCase.expectedTopic)
			b, _ := json.Marshal(value)
			body, err := suite.getDesiredPropertyOfFeature(featureID, desiredProperty)
			require.NoError(suite.T(), err, "unable to get property")
			assert.Equal(suite.T(), string(b), strings.TrimSpace(string(body)), "desired property doesn't match")
			suite.removeTestFeatures()
		})
	}
}

func (suite *ldtDesiredPropertySuite) TestEventDeleteDesiredProperty() {
	command := things.NewCommand(suite.namespacedID).FeatureDesiredProperty(featureID, desiredProperty).Delete()
	expectedTopic := suite.twinEventTopicDeleted

	suite.createTestFeature(featureWithDesiredProperties, featureID)
	suite.executeCommandEvent("e", suite.messagesFilter, nil, command, suite.expectedPath, expectedTopic)

	body, err := suite.getDesiredPropertyOfFeature(featureID, desiredProperty)
	require.Error(suite.T(), err, fmt.Sprintf("Desired property with key: '%s' should have been deleted", desiredProperty))
	assert.Nil(suite.T(), body, "body should be nil")

}

func (suite *ldtDesiredPropertySuite) TestCommandResponseModifyOrCreateDesiredProperty() {
	tests := map[string]ldtTestCaseData{
		"test_create_desired_property": {
			command:            things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().FeatureDesiredProperty(featureID, desiredProperty).Modify(value),
			expectedStatusCode: 201,
			feature:            emptyFeature,
		},

		"test_modify_desired_property": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				FeatureDesiredProperty(featureID, desiredProperty).Modify(value),
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

func (suite *ldtDesiredPropertySuite) TestCommandResponseDeleteDesiredProperty() {
	command := things.NewCommand(suite.namespacedID).FeatureDesiredProperty(featureID, desiredProperty).Delete()
	suite.createTestFeature(featureWithDesiredProperties, featureID)
	response, err := suite.executeCommandResponse(command)
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 204, response.Status, "unexpected status code")

}

func (suite *ldtDesiredPropertySuite) TestCommandResponseRetrieveDesiredProperty() {
	command := things.NewCommand(suite.namespacedID).FeatureDesiredProperty(featureID, desiredProperty).Retrieve()
	suite.createTestFeature(featureWithDesiredProperties, featureID)
	response, err := suite.executeCommandResponse(command)
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 200, response.Status, "unexpected status code")
	body, _ := suite.getDesiredPropertyOfFeature(featureID, desiredProperty)
	b, _ := json.Marshal(response.Value)
	assert.Equal(suite.T(), string(b), strings.TrimSpace(string(body)))
}
