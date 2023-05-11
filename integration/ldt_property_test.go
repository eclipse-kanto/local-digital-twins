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

type ldtPropertySuite struct {
	localDigitalTwinsSuite

	messagesFilter string
	expectedPath   string
}

func (suite *ldtPropertySuite) SetupSuite() {
	suite.SetupLdtSuite()
	suite.messagesFilter = fmt.Sprintf("like(resource:path,'/features/%s/properties/*')", featureID)
	suite.expectedPath = fmt.Sprintf("/features/%s/properties/%s", featureID, property)
}

func (suite *ldtPropertySuite) TearDownSuite() {
	suite.TearDownLdtSuite()
	suite.TearDown()
}

func TestPropertySuite(t *testing.T) {
	suite.Run(t, new(ldtPropertySuite))
}

func (suite *ldtPropertySuite) TestEventModifyOrCreateProperty() {
	tests := map[string]ldtTestCaseData{
		"test_create_property": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().FeatureProperty(featureID, property).Modify(value),
			expectedTopic: suite.twinEventTopicCreated,
			feature:       emptyFeature,
		},

		"test_modify_property": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				FeatureProperty(featureID, property).Modify(value),
			expectedTopic: suite.twinEventTopicModified,
			feature:       featureWithProperties,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			suite.createTestFeature(testCase.feature, featureID)
			suite.executeCommand("e", suite.messagesFilter, value, testCase.command, suite.expectedPath, testCase.expectedTopic)
			b, _ := json.Marshal(value)
			body, err := suite.getPropertyOfFeature(featureID, property)
			require.NoError(suite.T(), err, "unable to get property")
			assert.Equal(suite.T(), string(b), strings.TrimSpace(string(body)), "property doesn't match")
			suite.removeTestFeatures()
		})
	}
}
func (suite *ldtPropertySuite) TestEventDeleteProperty() {
	command := things.NewCommand(suite.namespacedID).Twin().FeatureProperty(featureID, property).Delete()
	expectedTopic := suite.twinEventTopicDeleted
	suite.createTestFeature(featureWithProperties, featureID)
	suite.executeCommand("e", suite.messagesFilter, nil, command, suite.expectedPath, expectedTopic)
	body, err := suite.getPropertyOfFeature(featureID, property)
	require.Error(suite.T(), err, fmt.Sprintf("Property with key: '%s' should have been deleted", property))
	assert.Nil(suite.T(), body, "body should be nil")
}
