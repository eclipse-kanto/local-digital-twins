// Copyright (c) 2020 Contributors to the Eclipse Foundation
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

package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
)

// TopicCriterion is a representation of the defined by Ditto topic criterion options.
type TopicCriterion string

const (
	// CriterionCommands represents the commands topic criterion.
	CriterionCommands TopicCriterion = "commands"
	// CriterionEvents represents the events topic criterion.
	CriterionEvents TopicCriterion = "events"
	// CriterionSearch represents the search topic criterion.
	CriterionSearch TopicCriterion = "search"
	// CriterionMessages represents the messages topic criterion.
	CriterionMessages TopicCriterion = "messages"
	// CriterionErrors represents the errors topic criterion.
	CriterionErrors TopicCriterion = "errors"
)

// TopicChannel is a representation of the defined by Ditto topic channel options.
type TopicChannel string

const (
	// ChannelTwin represents the twin channel topic.
	ChannelTwin TopicChannel = "twin"
	// ChannelLive represents the live channel topic.
	ChannelLive TopicChannel = "live"
)

// TopicAction is a representation of the defined by Ditto topic action options.
type TopicAction string

// Action constants.
const (
	ActionCreate    TopicAction = "create"
	ActionCreated   TopicAction = "created"
	ActionModify    TopicAction = "modify"
	ActionModified  TopicAction = "modified"
	ActionMerge     TopicAction = "merge"
	ActionMerged    TopicAction = "merged"
	ActionDelete    TopicAction = "delete"
	ActionDeleted   TopicAction = "deleted"
	ActionRetrieve  TopicAction = "retrieve"
	ActionSubscribe TopicAction = "subscribe"
	ActionRequest   TopicAction = "request"
	ActionCancel    TopicAction = "cancel"
	ActionNext      TopicAction = "next"
	ActionComplete  TopicAction = "complete"
	ActionFailed    TopicAction = "failed"
)

// TopicGroup is a representation of the defined by Ditto topic group options.
type TopicGroup string

// Group constants.
const (
	GroupThings   TopicGroup = "things"
	GroupPolicies TopicGroup = "policies"
)

// TopicPlaceholder can be used in the context of "any" for things namespaces and IDs in the retrieve topics.
const TopicPlaceholder = "_"

const (
	topicFormatPolicies       = "%s/%s/%s/%s/%s"
	topicFormatThings         = "%s/%s/%s/%s/%s/%s"
	topicFormatThingsNoAction = "%s/%s/%s/%s/%s"
)

// Topic represents the Ditto protocol's Topic entity. It's represented in the form of:
// <namespace>/<entityID>/<group>/<channel>/<criterion>/<action>.
// Each of the components is configurable based on the Ditto's specification for the specific group and/or channel/criterion/etc.
type Topic struct {
	Namespace string
	EntityID  string
	Group     TopicGroup
	Channel   TopicChannel
	Criterion TopicCriterion
	Action    TopicAction
}

// String provides the string representation of a Topic entity.
func (topic *Topic) String() string {
	switch topic.Group {
	case GroupThings:
		if len(topic.Action) == 0 {
			return fmt.Sprintf(topicFormatThingsNoAction, topic.Namespace, topic.EntityID, topic.Group, topic.Channel, topic.Criterion)
		}
		return fmt.Sprintf(topicFormatThings, topic.Namespace, topic.EntityID, topic.Group, topic.Channel, topic.Criterion, topic.Action)
	case GroupPolicies:
		return fmt.Sprintf(topicFormatPolicies, topic.Namespace, topic.EntityID, topic.Group, topic.Criterion, topic.Action)
	default:
		return ""
	}
}

// MarshalJSON marshals topic value.
func (topic *Topic) MarshalJSON() ([]byte, error) {
	return json.Marshal(topic.String())
}

// UnmarshalJSON unmarshals topic value.
func (topic *Topic) UnmarshalJSON(data []byte) error {
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	elements := strings.Split(v, "/")

	index := 0
	ns := elements[index]
	index++
	ID := elements[index]
	if err := validateNamespacedID(ns, ID); err != nil {
		return err
	}
	topic.Namespace = ns
	topic.EntityID = ID

	index++
	topic.Group = TopicGroup(elements[index])

	index++
	switch topic.Group {
	case GroupThings:
		topic.Channel = TopicChannel(elements[index])
		index++
	default:
		// skip channel - not supported for policies group
		topic.Channel = ""
	}
	topic.Criterion = TopicCriterion(elements[index])
	if index < len(elements)-1 {
		index++
		topic.Action = TopicAction(elements[index])
	} else {
		topic.Action = ""
	}

	return nil
}

func validateNamespacedID(ns, entityID string) error {
	var nsID *model.NamespacedID
	if ns == TopicPlaceholder {
		if entityID == TopicPlaceholder {
			return nil
		}
		nsID = model.NewNamespacedID("ns", entityID)
	} else {
		nsID = model.NewNamespacedID(ns, entityID)
	}

	if nsID == nil {
		return errors.New("invalid topic namespaced ID, namespace: " + ns + ", entity name: " + entityID)
	}

	return nil
}

// WithNamespace configures the namespace of the Topic.
func (topic *Topic) WithNamespace(ns string) *Topic {
	topic.Namespace = ns
	return topic
}

// WithEntityID configures the entity ID of the Topic.
func (topic *Topic) WithEntityID(entityID string) *Topic {
	topic.EntityID = entityID
	return topic
}

// WithGroup configures the TopicGroup of the Topic.
func (topic *Topic) WithGroup(group TopicGroup) *Topic {
	topic.Group = group
	return topic
}

// WithChannel configures the TopicChannel of the Topic.
func (topic *Topic) WithChannel(channel TopicChannel) *Topic {
	topic.Channel = channel
	return topic
}

// WithCriterion configures the TopicCriterion of the Topic.
func (topic *Topic) WithCriterion(criterion TopicCriterion) *Topic {
	topic.Criterion = criterion
	return topic
}

// WithAction configures the TopicAction of the Topic.
func (topic *Topic) WithAction(action TopicAction) *Topic {
	topic.Action = action
	return topic
}
