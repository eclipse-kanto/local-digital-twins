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

package commands

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol/things"
	"github.com/eclipse-kanto/suite-connector/logger"
	"github.com/pkg/errors"
)

// Scope type is used to define the command target resource type.
type Scope int

// Scope types.
const (
	ScopeUnknown Scope = iota

	ScopeThing
	ScopeAttributes // unsupported
	ScopeDefinition // unsupported
	ScopePolicy     // unsupported

	ScopeFeatures
	ScopeFeature
	ScopeFeatureProperties
	ScopeFeatureProperty
	ScopeFeatureDesiredProperties
	ScopeFeatureDesiredProperty
	ScopeFeatureDefinition // unsupported
)

const (
	pathProperties        = "/properties"
	pathDesiredProperties = "/desiredProperties"

	topicCmdEventFormat              = "command//%s:%s/req//%s"
	topicCmdResponseFormat           = "command//%s:%s/req//%s-response"
	topicCmdEventFormatRootDevice    = "command///req//%s"
	topicCmdResponseFormatRootDevice = "command///req//%s-response"

	topicEventFormat     = "e/%s/%s"
	topicEventRootDevice = "e"

	noValue = ""
)

// ParseCmdPath parses a ditto protocol path finding the type and name of the thing referenced resource
// returning the command SCOPE, id and additional path,
// e.g. ScopeFeatureProperties as scope, "sensor" as id and "temperature/value" as internal path
// if the command path is "/features/sensor/properties/temperature/value".
func ParseCmdPath(path string) (Scope, string, string) {
	separators := strings.Count(path, "/")

	if strings.HasPrefix(path, things.PathThingFeatures) {
		return parseFeaturesPath(path, separators)

	} else if separators == 1 {
		if path == things.PathThing {
			return ScopeThing, noValue, noValue // /
		} else if path == things.PathThingAttributes {
			return ScopeAttributes, noValue, noValue // /attributes
		} else if path == things.PathThingDefinition {
			return ScopeDefinition, noValue, noValue // /definition
		} else if path == things.PathThingPolicyID {
			return ScopePolicy, noValue, noValue // /policyId
		} else {
			return ScopeUnknown, noValue, noValue
		}

	} else if strings.HasPrefix(path, things.PathThingAttributes) {
		return ScopeAttributes, path[len(things.PathThingAttributes):], noValue // /attributes/<attributePath>

	} else {
		return ScopeUnknown, noValue, noValue
	}
}

func parseFeaturesPath(path string, separators int) (Scope, string, string) {
	start := len(things.PathThingFeatures)
	if separators == 1 {
		if len(path) == start {
			return ScopeFeatures, noValue, noValue // /features
		}

	} else {
		start = start + 1
		if separators == 2 {
			return ScopeFeature, path[start:], noValue // /features/<featureID>
		}

		next := start + strings.IndexRune(path[start:], '/')
		featureID := path[start:next]
		if separators == 3 {
			switch path[next:] {
			case things.PathThingDefinition:
				return ScopeFeatureDefinition, featureID, noValue // /features/<featureID>/definition
			case pathProperties:
				return ScopeFeatureProperties, featureID, noValue // /features/<featureID>/properties
			case pathDesiredProperties:
				return ScopeFeatureDesiredProperties, featureID, noValue // /features/<featureID>/desiredProperties
			default:
				return ScopeUnknown, noValue, noValue
			}
		}

		start = next + 1
		next = start + strings.IndexRune(path[start:], '/')
		if path[start-1:next] == pathProperties {
			// /features/<featureID>/properties/<propertyPath>
			return ScopeFeatureProperty, featureID, path[next:]
		}
		if path[start-1:next] == pathDesiredProperties {
			// /features/<featureID>/desiredProperties/<propertyPath>
			return ScopeFeatureDesiredProperty, featureID, path[next:]
		}
	}

	return ScopeUnknown, noValue, noValue
}

// TopicNamespaceID returns the namespace defined by the provided topic.
func TopicNamespaceID(topic *protocol.Topic) string {
	return fmt.Sprintf("%s:%s", topic.Namespace, topic.EntityID)
}

func commandValue(cmd *protocol.Envelope, value interface{}, out *CommandOutput) error {
	if err := json.Unmarshal(cmd.Value, &value); err != nil {
		out.invalidValueError = errors.Wrap(err, "invalid command payload")
		if cmd.Headers.ResponseRequired() {
			out.response = NewInvalidJSONValueError(cmd, err)
		}
		return err
	}
	return nil
}

func commandUnknownError(msg string, err error, cmd *protocol.Envelope, logger logger.Logger) *protocol.Envelope {
	logCmdError(msg, err, cmd, logger)

	if cmd.Headers.ResponseRequired() {
		return NewUnknownError(cmd, msg, err)
	}
	return nil
}

func commandPropertyNotFoundError(msg string, err error, cmd *Command, desired bool, logger logger.Logger,
) *protocol.Envelope {
	logCmdError(msg, err, cmd.envelope, logger)

	if cmd.envelope.Headers.ResponseRequired() {
		return NewPropertyNotFoundError(cmd.envelope, cmd.thingID, cmd.target, cmd.path, desired)
	}
	return nil
}

func publishEvent(h *Handler, event *protocol.Envelope) {
	if data, err := json.Marshal(event); err != nil {
		logCmdError("Unable to publish unexpected event", err, event, h.Logger)
	} else {
		message := message.NewMessage(watermill.NewUUID(), []byte(data))
		h.MosquittoPub.Publish(EventPublishTopic(h.DeviceID, event.Topic), message)
	}
}

// EventPublishTopic builds the message topic from the provided event envelope topic.
func EventPublishTopic(deviceID string, topic *protocol.Topic) string {
	if deviceID == TopicNamespaceID(topic) {
		return fmt.Sprintf(topicCmdEventFormatRootDevice, topic.Action)
	}
	return fmt.Sprintf(topicCmdEventFormat, topic.Namespace, topic.EntityID, topic.Action)
}

func publishResponse(h *Handler, response *protocol.Envelope) {
	if data, err := json.Marshal(response); err != nil {
		logCmdError("Unable to publish unexpected respose", err, response, h.Logger)
	} else {
		message := message.NewMessage(watermill.NewUUID(), []byte(data))
		h.MosquittoPub.Publish(ResponsePublishTopic(h.DeviceID, response.Topic), message)
	}
}

// ResponsePublishTopic builds the message topic from the provided response envelope topic.
func ResponsePublishTopic(deviceID string, envTopic *protocol.Topic) string {
	if deviceID == TopicNamespaceID(envTopic) {
		if len(envTopic.Action) == 0 {
			return fmt.Sprintf(topicCmdResponseFormatRootDevice, envTopic.Criterion)
		}
		return fmt.Sprintf(topicCmdResponseFormatRootDevice, envTopic.Action)
	}
	if len(envTopic.Action) == 0 {
		return fmt.Sprintf(topicCmdResponseFormat, envTopic.Namespace, envTopic.EntityID, envTopic.Criterion)
	}
	return fmt.Sprintf(topicCmdResponseFormat, envTopic.Namespace, envTopic.EntityID, envTopic.Action)
}

func responseHeaders(cmdHeaders *protocol.Headers) *protocol.Headers {
	var headers *protocol.Headers
	if cmdHeaders == nil {
		headers = protocol.NewHeaders()
	} else {
		headers = cmdHeaders.Clone()
	}
	return headers.WithResponseRequired(false)
}

func responseHeadersWithContent(cmdHeaders *protocol.Headers) *protocol.Headers {
	return responseHeaders(cmdHeaders).
		WithContentType(protocol.ContentTypeDitto)
}

// ResponseEnvelopeWithValue builds a response envelope.
func ResponseEnvelopeWithValue(
	cmdEnvelope *protocol.Envelope, status int, value interface{},
) *protocol.Envelope {
	if cmdEnvelope.Headers.ResponseRequired() {
		response := &protocol.Envelope{
			Topic:  cmdEnvelope.Topic,
			Path:   cmdEnvelope.Path,
			Fields: cmdEnvelope.Fields,
			Status: status,
		}
		return response.
			WithHeaders(responseHeadersWithContent(cmdEnvelope.Headers)).
			WithValue(value)
	}
	return nil
}

func responseEnvelope(cmdEnvelope *protocol.Envelope, status int) *protocol.Envelope {
	if cmdEnvelope.Headers.ResponseRequired() {
		response := &protocol.Envelope{
			Topic:  cmdEnvelope.Topic,
			Path:   cmdEnvelope.Path,
			Status: status,
		}
		return response.WithHeaders(responseHeaders(cmdEnvelope.Headers))
	}
	return nil
}

func eventEnvelope(
	cmdEnvelope *protocol.Envelope, revision int64, timestamp string, action protocol.TopicAction,
) *protocol.Envelope {
	env := &protocol.Envelope{
		Topic:     eventTopic(cmdEnvelope.Topic, action),
		Path:      cmdEnvelope.Path,
		Revision:  revision,
		Timestamp: timestamp,
	}

	if action != protocol.ActionDeleted {
		env.WithHeaders(responseHeadersWithContent(cmdEnvelope.Headers)).
			WithValue(cmdEnvelope.Value)
	} else {
		env.WithHeaders(responseHeaders(cmdEnvelope.Headers))
	}
	return env
}

func eventThingCreatedEnvelope(
	cmdEnvelope *protocol.Envelope, action protocol.TopicAction, thing *model.Thing,
) *protocol.Envelope {
	env := &protocol.Envelope{
		Topic:     eventTopic(cmdEnvelope.Topic, action),
		Path:      "/",
		Revision:  thing.Revision,
		Timestamp: thing.Timestamp,
	}

	env.WithHeaders(responseHeadersWithContent(cmdEnvelope.Headers)).
		WithValue(thing)
	return env
}

func eventTopic(topic *protocol.Topic, action protocol.TopicAction) *protocol.Topic {
	return &protocol.Topic{
		Namespace: topic.Namespace,
		EntityID:  topic.EntityID,
		Group:     topic.Group,
		Channel:   topic.Channel,
		Criterion: protocol.CriterionEvents,
		Action:    action,
	}
}

// PublishHonoMsg publishes the message with device to cloud messaging topic.
func PublishHonoMsg(msg *message.Message, publisher message.Publisher, dInfo DeviceInfo, thingID string) error {
	var pubTopic string
	if dInfo.DeviceID == thingID {
		pubTopic = topicEventRootDevice
	} else {
		pubTopic = fmt.Sprintf(topicEventFormat, dInfo.TenantID, thingID)
	}
	return publisher.Publish(pubTopic, msg)
}

func logCmdError(msg string, err error, cmd *protocol.Envelope, logger logger.Logger) {
	logger.Error(msg, err, CmdLogFields(cmd))
}

func logCmdHandled(cmd *protocol.Envelope, logger logger.Logger) {
	logger.Debug("Thing command handled", CmdLogFields(cmd))
}

func logCmdUnsupported(cmd *protocol.Envelope, logger logger.Logger) {
	logger.Trace("Thing command unsupported", CmdLogFields(cmd))
}

// CmdLogFields builds a command log data.
func CmdLogFields(cmd *protocol.Envelope) watermill.LogFields {
	return watermill.LogFields{
		"correlation-id": cmdCorrelationID(cmd),
		"path":           cmd.Path,
		"action":         cmd.Topic.Action,
		"thing":          TopicNamespaceID(cmd.Topic),
	}
}

func cmdCorrelationID(cmd *protocol.Envelope) string {
	if cmd.Headers == nil {
		return noValue
	}
	return cmd.Headers.CorrelationID()
}
