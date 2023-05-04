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

	"github.com/stretchr/testify/suite"

	"github.com/eclipse/ditto-clients-golang/model"
	"github.com/eclipse/ditto-clients-golang/protocol"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
	"github.com/stretchr/testify/require"
)

type ldtThingSuite struct {
	localDigitalTwinsSuite

	messagesFilter string
	expectedPath   string
}

func (suite *ldtThingSuite) SetupSuite() {
	suite.SetupLdtSuite()
	suite.messagesFilter = "like(resource:path,'/')"
	suite.expectedPath = "/"
}

func (suite *ldtThingSuite) TearDownSuite() {
	suite.TearDownLdtSuite()
	suite.TearDown()
}

func TestThingSuite(t *testing.T) {
	suite.Run(t, new(ldtThingSuite))
}

func (suite *ldtThingSuite) TestEventModifyOrCreateThing() {
	thing := &model.Thing{}
	thing.WithID(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID))
	thing.WithPolicyIDFrom(suite.ldtTestConfiguration.PolicyId)

	tests := map[string]struct {
		command        *things.Command
		expectedTopic  string
		beforeFunction func()
	}{
		"test_modify_thing": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Modify(thing),
			expectedTopic: suite.twinEventTopicModified,
		},
		"test_create_thing": {
			command:       things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Create(thing),
			expectedTopic: suite.twinEventTopicCreated,
			beforeFunction: func() {
				cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Delete()
				msg := cmd.Envelope(protocol.WithResponseRequired(false))
				err := suite.DittoClient.Send(msg)
				require.NoError(suite.T(), err, "removed test thing")
			},
		},
	}
	for testName, testCase := range tests {
		suite.Run(testName, func() {
			if testCase.beforeFunction != nil {
				testCase.beforeFunction()
			}
			suite.executeCommand("e", suite.messagesFilter, thing, testCase.command, suite.expectedPath, testCase.expectedTopic)
			b, _ := json.Marshal(thing)
			body, err := suite.getThing()
			require.NoError(suite.T(), err, "unable to get thing")
			assert.Equal(suite.T(), string(b), strings.TrimSpace(string(body)), "thing updated")
		})
	}
}

func (suite *ldtThingSuite) TestEventDeleteThing() {
	thing := &model.Thing{}
	thing.WithID(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID))
	thing.WithPolicyIDFrom(suite.ldtTestConfiguration.PolicyId)
	command := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Delete()
	expectedTopic := suite.twinEventTopicDeleted
	suite.executeCommand("e", suite.messagesFilter, nil, command, suite.expectedPath, expectedTopic)
	body, err := suite.getThing()
	require.Error(suite.T(), err, "thing should have been deleted")
	assert.Nil(suite.T(), body, "body should be nil")

	suite.createTestThing(thing)
}
