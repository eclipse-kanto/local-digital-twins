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
	"github.com/eclipse/ditto-clients-golang/protocol"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ldtThingSuite struct {
	localDigitalTwinsSuite

	thing          *model.Thing
	messagesFilter string
	expectedPath   string
}

func (suite *ldtThingSuite) SetupSuite() {
	suite.SetupLdtSuite()
	suite.messagesFilter = "like(resource:path,'/')"
	suite.expectedPath = "/"

	suite.thing = (&model.Thing{}).WithID(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).WithPolicyIDFrom(suite.ldtTestConfiguration.PolicyId)
}

func (suite *ldtThingSuite) TearDownSuite() {
	suite.TearDownLdtSuite()
	suite.TearDown()
}

func TestThingSuite(t *testing.T) {
	suite.Run(t, new(ldtThingSuite))
}

func (suite *ldtThingSuite) TestEventModifyOrCreateThing() {

	tests := map[string]ldtTestCaseData{
		"test_modify_thing": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Modify(suite.thing),
			expectedTopic: suite.twinEventTopicModified,
		},
		"test_create_thing": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Create(suite.thing),
			expectedTopic: suite.twinEventTopicCreated,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.command.Topic.Action == protocol.ActionCreate {
				suite.removeTestThing()
			}
			suite.executeCommandEvent("e", suite.messagesFilter, suite.thing, testCase.command, suite.expectedPath, testCase.expectedTopic)
			expectedBody, err := json.Marshal(suite.thing)
			require.NoError(suite.T(), err, "unable to marshal the expected body")

			actualBody, err := suite.getThing()
			require.NoError(suite.T(), err, "unable to get thing")

			assert.True(suite.T(), reflect.DeepEqual(suite.convertToMap(expectedBody), suite.convertToMap(actualBody)))
		})
	}
}

func (suite *ldtThingSuite) TestEventDeleteThing() {
	suite.executeCommandEvent("e", suite.messagesFilter, nil, things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Delete(), suite.expectedPath, suite.twinEventTopicDeleted)
	body, err := suite.getThing()
	require.Error(suite.T(), err, "thing should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")

	suite.createTestThing(suite.thing)
}

func (suite *ldtThingSuite) TestCommandResponseModifyOrCreateThing() {
	tests := map[string]ldtTestCaseData{
		"test_create_thing": {
			command:            things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Create(suite.thing),
			expectedStatusCode: 201,
		},

		"test_modify_thing": {
			command:            things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Modify(suite.thing),
			expectedStatusCode: 204,
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.command.Topic.Action == protocol.ActionCreate {
				suite.removeTestThing()
			}
			response, err := suite.executeCommandResponse(testCase.command)
			require.NoError(suite.T(), err, "could not get response")
			assert.Equal(suite.T(), testCase.expectedStatusCode, response.Status, "unexpected status code")
		})
	}
}

func (suite *ldtThingSuite) TestCommandResponseDeleteThing() {
	response, err := suite.executeCommandResponse(things.NewCommand(suite.namespacedID).Delete())
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 204, response.Status, "unexpected status code")
	suite.createTestThing(suite.thing)
}

func (suite *ldtThingSuite) TestCommandResponseRetrieveThing() {
	response, err := suite.executeCommandResponse(things.NewCommand(suite.namespacedID).Retrieve())
	require.NoError(suite.T(), err, "could not get response")
	assert.Equal(suite.T(), 200, response.Status, "unexpected status code")
	actualBody, err := suite.getThing()
	require.NoError(suite.T(), err, "unable to get thing")
	assert.True(suite.T(), reflect.DeepEqual(response.Value, suite.convertToMap(actualBody)))
}
