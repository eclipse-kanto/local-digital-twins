// Copyright (c) 2022 Contributors to the Eclipse Foundation
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

//go:build integration_hub
// +build integration_hub

// Note: This is an integration test of local application messages processing by the local digital twin. It requires a
// running twin with connection to mosquitto (mqtt) and does not require a cloud connection.

package commands_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol/things"
	"github.com/eclipse-kanto/local-digital-twins/internal/testutil"
	"github.com/eclipse-kanto/suite-connector/connector"
)

const (
	defaultTimeout = 10 * time.Second

	noStatus = -1
)

var (
	defHeader      *protocol.Headers
	eventTopic     *protocol.Topic
	itTestThingCmd *things.Command
)

type MessagingSuite struct {
	suite.Suite
	client   *testutil.MessageClient
	thingID  *model.NamespacedID
	pubTopic string
}

func TestMessageTestSuite(t *testing.T) {
	suite.Run(t, new(MessagingSuite))
}

func (s *MessagingSuite) SetupSuite() {
	t := s.T()
	var err error

	c, err := testutil.NewMessageClient()
	require.NoError(t, err)

	thingName := c.Subscription.DeviceName + ":sensor"
	_, err = c.WithSub(thingName)
	require.NoError(t, err)

	s.client = c

	s.thingID = model.NewNamespacedID(s.client.Subscription.Namespace, thingName)

	s.pubTopic = fmt.Sprintf("e/%s/%s", s.client.Subscription.TenantID, s.thingID)

	defHeader = protocol.NewHeaders().WithCorrelationID("test/local-digital-twins/commands")
	s.setupTestThing()
}

func (s *MessagingSuite) setupTestThing() {
	itTestThingCmd = things.NewCommand(s.thingID)
	itTestThingCmd.Retrieve()

	s.publishCommand(itTestThingCmd)
	receivedMessages := s.assertPublishedCount(1)

	env := asEnvelope(s.T(), receivedMessages[0].Payload)

	if env.Status != 200 {
		thing := model.Thing{
			ID:       s.thingID,
			PolicyID: s.thingID,
		}
		itTestThingCmd.Create(&thing)
		s.publishCommand(itTestThingCmd)
		s.assertPublishedCount(2)
	}

	eventTopic = (&protocol.Topic{}).
		WithNamespace(s.client.Subscription.Namespace).
		WithEntityID(s.thingID.Name).
		WithGroup(protocol.GroupThings).
		WithChannel(protocol.ChannelTwin).
		WithCriterion(protocol.CriterionEvents)
}

func (s *MessagingSuite) TearDownSuite() {
	s.client.Pub.Close()
	s.client.Sub.Close()
	s.client.Connection.Disconnect()
}

func (s *MessagingSuite) TestFeatureOperations() {
	itTestThingCmd.Feature("square")
	eventTopic.WithCriterion(protocol.CriterionEvents)

	// response & event
	itTestThingCmd.Modify(model.Feature{
		Properties: map[string]interface{}{"a": 5},
	})
	s.publishCommand(itTestThingCmd)
	s.assertPublished(201, itTestThingCmd.Topic,
		eventTopic.WithAction(protocol.ActionCreated))

	// response
	itTestThingCmd.Retrieve()
	s.publishCommand(itTestThingCmd)
	s.assertPublished(200, itTestThingCmd.Topic)

	// event
	itTestThingCmd.Delete()
	s.publishCommandNoResponseRequired(itTestThingCmd)
	s.assertPublished(noStatus, eventTopic.WithAction(protocol.ActionDeleted))
}

func (s *MessagingSuite) TestFeatureNotFoundError() {
	itTestThingCmd.Feature("unexisting")
	itTestThingCmd.Retrieve()

	s.publishCommand(itTestThingCmd)
	s.assertPublishedOnError(404)
}

func (s *MessagingSuite) TestInvalidCommandPayload() {
	s.publish([]byte("{invalidCmdPayload"))
	s.assertPublishedCount(0) // Expected ERROR in the log file and no messages published
}

func (s *MessagingSuite) publishCommand(cmd *things.Command) {
	defHeader.WithResponseRequired(true)
	s.publishEnv(cmd.Envelope(defHeader))
}

func (s *MessagingSuite) publishCommandNoResponseRequired(cmd *things.Command) {
	defHeader.WithResponseRequired(false)
	s.publishEnv(cmd.Envelope(defHeader))
}

func (s *MessagingSuite) publishEnv(env *protocol.Envelope) {
	data, err := json.Marshal(env)
	require.NoError(s.T(), err)
	s.publish(data)
}

func (s *MessagingSuite) publish(data []byte) {
	pubMsg := message.NewMessage("i_test", data)
	require.NoError(s.T(), s.client.Pub.Publish(s.pubTopic, pubMsg))
}

func (s *MessagingSuite) assertPublished(
	cmdStatus int, topics ...*protocol.Topic,
) {
	receivedMessages := s.assertPublishedCount(len(topics))

	for msgIndex, topic := range topics {
		if topic.Criterion == protocol.CriterionCommands {
			s.assertPublishedMessage(receivedMessages[msgIndex], cmdStatus, topic)
		} else {
			s.assertPublishedMessage(receivedMessages[msgIndex], noStatus, topic)
		}
	}
}

func (s *MessagingSuite) assertPublishedOnError(errorStatus int) {
	receivedMessages := s.assertPublishedCount(1)

	eventTopic.WithCriterion(protocol.CriterionErrors).WithAction("")
	s.assertPublishedMessage(receivedMessages[0], errorStatus, eventTopic)
}

func (s *MessagingSuite) assertPublishedCount(count int) message.Messages {
	receivedMessages, all := subscriber.BulkRead(s.client.MessagesCh, count, defaultTimeout)
	assert.True(s.T(), all, fmt.Sprintf("Expected %d messages, but found %d", count, len(receivedMessages)))
	return receivedMessages
}

func (s *MessagingSuite) assertPublishedMessage(receivedMessage *message.Message,
	expStatus int, expTopic *protocol.Topic,
) {
	format := "command//" + s.thingID.String() + "/req//%s"

	var expMqttTopic string
	if expTopic.Criterion == protocol.CriterionEvents {
		expMqttTopic = fmt.Sprintf(format, expTopic.Action)
	} else {
		if len(expTopic.Action) == 0 {
			expMqttTopic = fmt.Sprintf(format, expTopic.Criterion) + "-response"
		} else {
			expMqttTopic = fmt.Sprintf(format, expTopic.Action) + "-response"
		}
	}
	if actualMqttTopic, ok := connector.TopicFromCtx(receivedMessage.Context()); ok {
		assert.Equal(s.T(), expMqttTopic, actualMqttTopic)
	} else {
		assert.Fail(s.T(), "Topic missing from context")
	}
	assertMessageContent(s.T(), receivedMessage.Payload, expTopic, expStatus)
}

// Match by topic, path and status. NO_STATUS is passed when the message doesn't contain status code (events).
func assertMessageContent(
	t *testing.T, actualPayload message.Payload, expTopic *protocol.Topic, expStatus int,
) {
	env := asEnvelope(t, actualPayload)
	assert.EqualValues(t, expTopic, env.Topic)

	if expStatus >= 400 {
		assert.EqualValues(t, "/", env.Path)
	} else {
		assert.EqualValues(t, itTestThingCmd.Path, env.Path)
	}

	if expStatus != noStatus {
		assert.EqualValues(t, expStatus, env.Status)
	}
}

func asEnvelope(t *testing.T, payload message.Payload) protocol.Envelope {
	env := protocol.Envelope{}
	require.NoError(t, json.Unmarshal(payload, &env))
	return env
}
