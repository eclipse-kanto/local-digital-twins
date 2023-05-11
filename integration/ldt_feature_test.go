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
	"strings"
	"testing"

	"github.com/eclipse/ditto-clients-golang/protocol/things"
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
			command: things.NewCommand(suite.namespacedID).Twin().
				Feature(featureID).Modify(emptyFeature),
			expectedTopic: suite.twinEventTopicCreated,
		},

		"test_modify_feature": {
			command: things.NewCommand(suite.namespacedID).Twin().
				Feature(featureID).Modify(emptyFeature),
			expectedTopic: suite.twinEventTopicModified,
			feature:       emptyFeature,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.feature != nil {
				suite.createTestFeature(testCase.feature, featureID)
			}
			suite.executeCommand("e", suite.messagesFilter, emptyFeature, testCase.command, suite.expectedPath, testCase.expectedTopic)
			b, _ := json.Marshal(emptyFeature)
			body, err := suite.getFeature(featureID)
			require.NoError(suite.T(), err, "unable to get feature")
			assert.Equal(suite.T(), string(b), strings.TrimSpace(string(body)), "feature doesn't match")
			suite.removeTestFeatures()
		})
	}
}

func (suite *ldtFeatureSuite) TestEventDeleteFeature() {
	command := things.NewCommand(suite.namespacedID).Twin().Feature(featureID).Delete()
	expectedTopic := suite.twinEventTopicDeleted

	suite.createTestFeature(emptyFeature, featureID)
	suite.executeCommand("e", suite.messagesFilter, nil, command, suite.expectedPath, expectedTopic)

	body, err := suite.getFeature(featureID)
	require.Error(suite.T(), err, "feature should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")
}
