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

package testutil

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/suite-connector/connector"
	"github.com/eclipse-kanto/suite-connector/logger"
	"github.com/eclipse-kanto/suite-connector/testutil"
	"github.com/pkg/errors"
)

const (
	subTopicRootDevice          = "command///req/#"
	subTopicFormatVirtualDevice = "command//%s/req/#"
)

// MessageClient contains the testing messaging related data.
type MessageClient struct {
	Subscription *SubscriptionDetails
	MessagesCh   <-chan *message.Message
	Pub          message.Publisher
	Sub          message.Subscriber
	Connection   *connector.MQTTConnection

	logger logger.Logger
}

// NewMessageClient returns a new MessageClient with an initialized testutil.SubscriptionDetails, local MQTT connection
// and publisher.
func NewMessageClient(t *testing.T) (*MessageClient, error) {
	subsDetails, err := readSubscriptionTestData()
	if err != nil {
		return nil, err
	}

	c := &MessageClient{
		Subscription: subsDetails,
	}

	logger := testutil.NewLogger("integration", logger.DEBUG, t)
	c.logger = logger

	config, err := testutil.NewLocalConfig()
	if err != nil {
		return nil, err
	}

	c.Connection, err = connector.NewMQTTConnection(config, watermill.NewShortUUID(), logger)
	if err != nil {
		return nil, err
	}

	future := c.Connection.Connect()
	<-future.Done()
	err = future.Error()
	if err != nil {
		return nil, err
	}

	c.Pub = connector.NewPublisher(c.Connection, connector.QosAtLeastOnce, logger, nil)

	return c, nil
}

// WithSub initializes the client subscriber and message channel. The thing names are used to build the things`
// model.NamespacedID and subscription topic.
func (c *MessageClient) WithSub(thingNames ...string) (*MessageClient, error) {
	if len(thingNames) == 0 {
		return nil, errors.New("thing name expected but not provided")
	}

	c.Sub = connector.NewSubscriber(c.Connection, connector.QosAtMostOnce, true, c.logger, nil)

	subTopic := subTopic(c.Subscription, thingNames...)

	messages, err := c.Sub.Subscribe(context.Background(), subTopic)
	if err != nil {
		return nil, err
	}
	c.MessagesCh = messages

	return c, nil
}

func subTopic(subscription *SubscriptionDetails, thingNames ...string) string {
	rootThingID := model.NewNamespacedID(subscription.Namespace, subscription.DeviceName)
	if len(thingNames) == 1 {
		thingID := model.NewNamespacedID(subscription.Namespace, thingNames[0])
		return subTopicType(thingID, rootThingID)
	}

	subTopics := make([]string, len(thingNames))
	for i, name := range thingNames {
		thingID := model.NewNamespacedID(subscription.Namespace, name)
		subTopics[i] = subTopicType(thingID, rootThingID)
	}

	return strings.Join(subTopics, ",")
}

func subTopicType(thingID, rootThingID *model.NamespacedID) string {
	if *thingID == *rootThingID {
		return subTopicRootDevice
	}
	return fmt.Sprintf(subTopicFormatVirtualDevice, thingID)
}

// Publish creates a new message.Message and publishes it using the client publisher.
func (c *MessageClient) Publish(pubTopic string, data []byte) error {
	pubMsg := message.NewMessage("i_test_msg", data)
	return c.Pub.Publish(pubTopic, pubMsg)
}
