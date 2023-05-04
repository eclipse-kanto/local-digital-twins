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

	"github.com/stretchr/testify/suite"

	"github.com/eclipse/ditto-clients-golang/model"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
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
	properties := map[string]interface{}{desiredProperty: value}
	tests := map[string]struct {
		command        *things.Command
		expectedTopic  string
		beforeFunction func()
	}{
		"test_create_desired_properties": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().FeatureDesiredProperties(featureID).Modify(properties),
			expectedTopic: suite.twinEventTopicCreated,
			beforeFunction: func() {
				suite.createTestFeature(emptyFeature, featureID)
			},
		},

		"test_modify_desired_properties": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				FeatureDesiredProperties(featureID).Modify(properties),
			expectedTopic: suite.twinEventTopicModified,
			beforeFunction: func() {
				suite.createTestFeature(featureWithDesiredProperties, featureID)
			},
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.beforeFunction != nil {
				testCase.beforeFunction()
			}
			suite.executeCommand("e", suite.messagesFilter, properties, testCase.command, suite.expectedPath, testCase.expectedTopic)
			b, _ := json.Marshal(properties)
			body, err := suite.getAllDesiredPropertiesOfFeature(featureID)
			require.NoError(suite.T(), err, "unable to get desired properties")
			assert.Equal(suite.T(), string(b), strings.TrimSpace(string(body)), "desired properties don't match")
			suite.removeTestFeatures()
		})
	}
}

func (suite *ldtDesiredPropertiesSuite) TestEventDeleteDesiredProperties() {
	command := things.NewCommand(suite.namespacedID).FeatureDesiredProperties(featureID).Delete()
	expectedTopic := suite.twinEventTopicDeleted

	suite.createTestFeature(featureWithDesiredProperties, featureID)
	suite.executeCommand("e", suite.messagesFilter, nil, command, suite.expectedPath, expectedTopic)

	body, err := suite.getAllDesiredPropertiesOfFeature(featureID)
	require.Error(suite.T(), err, "desired properties of feature should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")

}
