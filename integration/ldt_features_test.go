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
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"

	"github.com/eclipse/ditto-clients-golang/model"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
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
	features := map[string]*model.Feature{featureID: emptyFeature}

	tests := map[string]ldtTestCaseData{
		"test_create_features": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				Features().Modify(features),
			expectedTopic: suite.twinEventTopicCreated,
		},

		"test_modify_features": {
			command: things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().
				Features().Modify(features),
			expectedTopic: suite.twinEventTopicModified,
			feature:       emptyFeature,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.feature != nil {
				suite.createTestFeature(testCase.feature, featureID)
			}
			suite.executeCommand("e", suite.messagesFilter, features, testCase.command, suite.expectedPath, testCase.expectedTopic)
			b, _ := json.Marshal(features)
			body, err := suite.getAllFeatures()
			require.NoError(suite.T(), err, "unable to get features")
			assert.Equal(suite.T(), string(b), strings.TrimSpace(string(body)), "features don't match")
			suite.removeTestFeatures()
		})
	}
}
func (suite *ldtFeaturesSuite) TestEventDeleteFeatures() {
	command := things.NewCommand(suite.namespacedID).Twin().Features().Delete()
	expectedTopic := suite.twinEventTopicDeleted

	suite.createTestFeature(emptyFeature, featureID)
	suite.executeCommand("e", suite.messagesFilter, nil, command, suite.expectedPath, expectedTopic)

	body, err := suite.getAllFeatures()
	require.Error(suite.T(), err, "features should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")
}
