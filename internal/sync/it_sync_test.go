// Copyright (c) 2022 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0
//
// SPDX-License-Identifier: EPL-2.0

//go:build integration_hub
// +build integration_hub

package sync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol/things"
	"github.com/eclipse-kanto/local-digital-twins/internal/testutil"
	"go.uber.org/goleak"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// Note: This is an integration test of the offline -> online synchronization between devices/apps and the cloud, so it
// requires a running twin with connections to mosquitto (mqtt) and the cloud. To stop/start the cloud connection, the
// test modifies the provisioning.json file used by the twin.
//
// In case the test is run outside of the docker container, then provDefaultPath and provNewPath need to be updated
// with proper provisioning.json locations.
// Check docker/docker-compose-integration-tests.yml for set up hub integration tests.

const (
	featureToModify         = "FeatureToModify"
	featureToDelete         = "FeatureToDelete"
	featureToAdd            = "FeatureToAdd"
	featureToMerge          = "FeatureToMerge"
	featureToModifyProperty = "ModifiedProperty"
	featureToDeleteProperty = "DeletedProperty"
	featureToAddProperty    = "AddedProperty"
	featureToMergeProperty  = "MergedProperty"

	pubTopicRootDevice          = "e"
	pubTopicVirtualDeviceFormat = "e/%s/%s"

	provDefaultPath = "/build/provisioning.json"
	provNewPath     = "/build/nonexistent.json"
)

type thingTestInfo struct {
	id       *model.NamespacedID
	cmd      *things.Command
	pubTopic string
}

type SyncSuite struct {
	suite.Suite
	test        testutil.HTTPTest
	client      *testutil.MessageClient
	messagesOut chan message.Messages

	rootInfo    thingTestInfo
	virtualInfo thingTestInfo

	defHeader *protocol.Headers
}

func TestSyncSuite(t *testing.T) {
	suite.Run(t, new(SyncSuite))
}

func (s *SyncSuite) SetupSuite() {
	test, err := testutil.NewHTTPTest()
	require.NoError(s.T(), err)
	s.test = test

	client, err := testutil.NewMessageClient(s.T())
	require.NoError(s.T(), err)

	rootThingName := client.Subscription.DeviceName
	virtualThingName := client.Subscription.DeviceName + ":virtual"

	_, err = client.WithSub(rootThingName, virtualThingName)
	require.NoError(s.T(), err)

	s.client = client

	rootThingID := model.NewNamespacedID(test.Subscription.Namespace, s.client.Subscription.DeviceName)
	s.rootInfo = thingTestInfo{
		id:       rootThingID,
		pubTopic: pubTopicRootDevice,
		cmd:      things.NewCommand(rootThingID),
	}

	virtualThingID := model.NewNamespacedID(test.Subscription.Namespace, virtualThingName)
	s.virtualInfo = thingTestInfo{
		id:       virtualThingID,
		pubTopic: fmt.Sprintf(pubTopicVirtualDeviceFormat, test.Subscription.TenantID, virtualThingID),
		cmd:      things.NewCommand(virtualThingID),
	}

	s.defHeader = protocol.NewHeaders().
		WithCorrelationID("tests:c_id").
		WithResponseRequired(true)

	s.messagesOut = make(chan message.Messages)

	s.setupThings()
}

func (s *SyncSuite) setupThings() {
	setup := func() error {
		features := map[string]*model.Feature{
			featureToModify: {
				DesiredProperties: map[string]interface{}{
					featureToModifyProperty: 30,
				},
			},
			featureToMerge: {
				DesiredProperties: map[string]interface{}{
					featureToMergeProperty: 7,
				},
			},
		}

		if err := s.setupThing(s.rootInfo, features); err != nil {
			return err
		}

		features = map[string]*model.Feature{
			featureToDelete: {
				DesiredProperties: map[string]interface{}{
					featureToDeleteProperty: -30,
				},
			},
		}

		if err := s.setupThing(s.virtualInfo, features); err != nil {
			return err
		}

		return nil
	}

	s.handle(setup, 4)
}

func (s *SyncSuite) setupThing(info thingTestInfo, features map[string]*model.Feature) error {
	thing := model.Thing{
		ID:       info.id,
		Features: features,
	}
	info.cmd.Modify(thing)
	return s.publishCmd(info)
}

func (s *SyncSuite) TearDownSuite() {
	s.rootInfo.cmd.Feature(featureToModify).
		Delete()
	require.NoError(s.T(), s.publishCmd(s.rootInfo))

	s.rootInfo.cmd.Feature(featureToMerge).
		Delete()
	require.NoError(s.T(), s.publishCmd(s.rootInfo))

	s.virtualInfo.cmd.Feature(featureToDelete).
		Delete()
	require.NoError(s.T(), s.publishCmd(s.virtualInfo))

	s.virtualInfo.cmd.Feature(featureToAdd).
		Delete()
	require.NoError(s.T(), s.publishCmd(s.virtualInfo))

	s.client.Pub.Close()
	s.client.Sub.Close()
	s.client.Connection.Disconnect()

	goleak.VerifyNone(s.T())
}

// Prerequisites for this test:
// 1. Either modify test_data.json with your own credentials, or create a new file and embed it in
//    subscription_details.go
// 2. Start mosquitto.
// 3. Place your provisioning.json in the same directory as the local digital twins executable is.
// 4. Start the twin exe with -localUsername,-localPassword,-deviceId and -tenantId flags.
func (s *SyncSuite) TestHubEdgeSynchronization() {
	renameProvisioning(false)

	changeRemoteState(s)
	changeLocalState(s)

	renameProvisioning(true)

	assertCloudEvent(s)

	assertLocalFeaturePresent(s, s.rootInfo, featureToModify, &model.Feature{})
	assertRemoteState(s)
}

// TestUnsupportedLocalCommand asserts that a ditto merge command which is not supported by LDT is successfully
// received by the hub.
func (s *SyncSuite) TestUnsupportedLocalCommand() {
	s.rootInfo.cmd.Feature(featureToMerge)

	s.rootInfo.cmd.Merge(model.Feature{
		Properties: map[string]interface{}{featureToMergeProperty: 6},
	})

	// publish locally unsupported command - merge action
	header := protocol.NewHeaders().
		WithCorrelationID("tests:c_id").
		WithResponseRequired(true).
		WithContentType("application/merge-patch+json")
	data, err := json.Marshal(s.rootInfo.cmd.Envelope(header))
	require.NoError(s.T(), err)
	require.NoError(s.T(), s.client.Publish(s.rootInfo.pubTopic, data))
	s.handle(nil, 0)

	// assert no local changes because of unsupported merge command
	expFeature := &model.Feature{
		DesiredProperties: map[string]interface{}{
			featureToMergeProperty: float64(7),
		},
	}
	assertLocalFeaturePresent(s, s.rootInfo, featureToMerge, expFeature)

	// assert that cloud feature state is updated because of the merge command
	req := s.test.NewThingsRequest(s.rootInfo.id.String()).
		Get().
		Feature(featureToMerge)

	resp, err := req.Execute()
	require.NoError(s.T(), err)

	feature := &model.Feature{}
	require.NoError(s.T(), json.Unmarshal(resp, feature))
	assert.EqualValues(s.T(), 6, feature.Properties[featureToMergeProperty])
}

func renameProvisioning(toDefault bool) {
	if toDefault {
		os.Rename(provNewPath, provDefaultPath)
	} else {
		os.Rename(provDefaultPath, provNewPath)
	}

	time.Sleep(6 * time.Second)
}

func assertRemoteState(s *SyncSuite) {
	req := s.test.NewThingsRequest(s.virtualInfo.id.String()).
		Get().
		Feature(featureToAdd)

	resp, err := req.Execute()
	require.NoError(s.T(), err)

	feature := model.Feature{}
	assert.NoError(s.T(), json.Unmarshal(resp, &feature))
	assert.EqualValues(s.T(), 100, feature.Properties[featureToAddProperty])

	resp, err = req.Feature(featureToDelete).
		Execute()
	require.NoError(s.T(), err)

	errNotFound := make(map[string]interface{})
	assert.NoError(s.T(), json.Unmarshal(resp, &errNotFound))
	status := int(errNotFound["status"].(float64))
	assert.Equal(s.T(), http.StatusNotFound, status)
}

func assertLocalFeaturePresent(s *SyncSuite, info thingTestInfo, featureId string, expected *model.Feature) {
	retrieveFeature := func() error {
		info.cmd.Feature(featureId).
			Retrieve()
		return s.publishCmd(info)
	}
	messages := s.handle(retrieveFeature, 1)

	env := asEnvelope(s.T(), messages[0].Payload)
	actual := &model.Feature{}
	require.NoError(s.T(), json.Unmarshal(env.Value, actual))

	assert.EqualValues(s.T(), expected, actual)
}

func assertCloudEvent(s *SyncSuite) {
	s.handle(nil, 1) // cloud event for first remote feature modify
}

func changeLocalState(s *SyncSuite) {
	changeLocalState := func() error {
		s.rootInfo.cmd.Feature(featureToModify).
			Modify(model.Feature{
				DesiredProperties: map[string]interface{}{
					featureToModifyProperty: 40,
				},
			})
		if err := s.publishCmd(s.rootInfo); err != nil {
			return err
		}

		s.virtualInfo.cmd.Feature(featureToDelete).
			Delete()
		if err := s.publishCmd(s.virtualInfo); err != nil {
			return err
		}

		s.virtualInfo.cmd.Feature(featureToAdd).
			Modify(model.Feature{
				Properties: map[string]interface{}{
					featureToAddProperty: 100,
				},
			})
		if err := s.publishCmd(s.virtualInfo); err != nil {
			return err
		}
		return nil
	}

	s.handle(changeLocalState, 6)
}

func changeRemoteState(s *SyncSuite) {
	feature := model.Feature{}
	req := s.test.NewThingsRequest(s.rootInfo.id.String()).
		Put(feature). // delete remote desired properties by putting empty feature
		Feature(featureToModify)

	_, err := req.Execute()
	require.NoError(s.T(), err)

	feature = model.Feature{DesiredProperties: map[string]interface{}{
		featureToDeleteProperty: -50,
	}}
	req = s.test.NewThingsRequest(s.virtualInfo.id.String()).
		Put(feature).
		Feature(featureToDelete)

	_, err = req.Execute()
	require.NoError(s.T(), err)
}

func (s *SyncSuite) publishCmd(info thingTestInfo) error {
	data, err := json.Marshal(info.cmd.Envelope(s.defHeader))
	if err != nil {
		return err
	}

	return s.client.Publish(info.pubTopic, data)
}

// 1. Polls the provided number of messages in a separate thread
// 2. Calls the function f if it's provided
// 3. Checks the number of received messages and returns them
func (s *SyncSuite) handle(f func() error, messageCount int) message.Messages {
	go s.pollMessagesCount(messageCount)

	if f != nil {
		err := f()
		require.NoError(s.T(), err)
	}

	messages := <-s.messagesOut
	s.assertPublishedCount(messages, messageCount)

	return messages
}

func (s *SyncSuite) assertPublishedCount(messages message.Messages, count int) {
	length := len(messages)
	assert.Equal(s.T(), count, length, fmt.Sprintf("Expected %d messages, but found %d", count, length))
}

func (s *SyncSuite) pollMessagesCount(count int) {
	receivedMessages, _ := subscriber.BulkRead(s.client.MessagesCh, count, 10*time.Second)
	s.messagesOut <- receivedMessages
}

func asEnvelope(t *testing.T, payload message.Payload) protocol.Envelope {
	env := protocol.Envelope{}
	require.NoError(t, json.Unmarshal(payload, &env))
	return env
}
