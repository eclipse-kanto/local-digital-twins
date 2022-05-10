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

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/suite-connector/connector"
	"github.com/eclipse-kanto/suite-connector/logger"
	"github.com/pkg/errors"
)

const (
	ok       = 200
	created  = 201
	modified = 204
	deleted  = 204
)

// DeviceInfo contains the gateway device details.
type DeviceInfo struct {
	DeviceID         string
	TenantID         string
	AutoProvisioning bool
}

// Handler manages ditto protocol commands using the local digital twins storage.
type Handler struct {
	DeviceInfo

	MosquittoPub message.Publisher
	HonoPub      message.Publisher
	Storage      persistence.ThingsStorage

	Logger logger.Logger
}

// Command contains the parsed command data used by CommandFunc to perform the ditto command.
//
// The thingID is parsed from the envelop topic.
// The target and the additional path (if any) are get form the envelope path,
// e.g. "sensor" as target and "temperature/value" as internal path
// if the envelope path is "/features/sensor/properties/temperature/value".
type Command struct {
	envelope *protocol.Envelope
	thingID  string
	target   string
	path     string
}

// CommandOutput contains response and event which must be published or invalid value error.
// In addition it could contain the thing/feature local revision that could be marked as synchronized
// if the command is successfully forwarded to the cloud.
type CommandOutput struct {
	response          *protocol.Envelope
	event             *protocol.Envelope
	invalidValueError error

	thingID   string
	featureID string
	revision  int64
}

// CommandFunc performs the passed Command using the provided Handler.
// Generates the command output, i.e. the response if required and the event to be published
// if the command is executed successfully.
type CommandFunc func(h *Handler, cmd *Command, out *CommandOutput)

// HandleCommand executes ditto protocol command using the local digital twins storage.
func (h *Handler) HandleCommand(msg *message.Message) ([]*message.Message, error) {
	command := &protocol.Envelope{}

	if err := json.Unmarshal(msg.Payload, &command); err != nil {
		return nil, errors.Wrap(err, "invalid command payload")
	}

	if command.Topic.Channel == protocol.ChannelTwin &&
		command.Topic.Criterion == protocol.CriterionCommands {

		cmdType, target, path := ParseCmdPath(command.Path)
		if cmdType == ScopeUnknown {
			return nil, errors.Errorf("invalid command path %s", command.Path)
		}

		var cmdFunc CommandFunc
		var cmd *Command

		if cmdType == ScopeThing {
			// commands with '/' path prefix
			cmdFunc = thingCommand(command.Topic.Action)
			cmd = &Command{
				envelope: command,
				thingID:  TopicNamespaceID(command.Topic),
			}
		}

		if cmdType >= ScopeFeatures {
			// all thing commands with '/features' path prefix
			cmdFunc = featuresPathCommand(command.Topic.Action, cmdType)
			cmd = &Command{
				envelope: command,
				thingID:  TopicNamespaceID(command.Topic),
				target:   target,
				path:     path,
			}
		}

		if cmdFunc == nil {
			logCmdUnsupported(command, h.Logger)
			return []*message.Message{msg}, nil
		}

		output := &CommandOutput{}
		cmdFunc(h, cmd, output)

		h.publishCommandLocalOutput(msg, command, output)
		if output.invalidValueError != nil {
			logCmdHandled(command, h.Logger)
			return nil, output.invalidValueError
		}

		logCmdHandled(command, h.Logger)
		err := h.publishCommandToHono(msg, command, output)
		if err == nil {
			h.Logger.Trace("Thing command forwarded to hono successfully", nil)
			h.resourceSynchronized(output)
		}
		return nil, nil
	}

	return []*message.Message{msg}, nil
}

func (h *Handler) publishCommandToHono(msg *message.Message, command *protocol.Envelope, output *CommandOutput) error {
	forwardMsg := msg
	if output.response != nil {
		// do not require response if already published
		if command.Topic.Action == protocol.ActionRetrieve {
			forwardMsg = cmdWithNoResponseRequired(msg, command)
		}
	}

	err := PublishHonoMsg(forwardMsg, h.HonoPub, h.DeviceInfo, TopicNamespaceID(command.Topic))
	if err != nil {
		if errors.Is(err, connector.ErrNotConnected) {
			h.Logger.Trace("Thing command not forwarded to hono: no hub connection", nil)
		} else {
			logCmdError("Thing command not forwarded to hono, unexpected error:", err, command, h.Logger)
		}
	}
	return err
}

func thingCommand(action protocol.TopicAction) CommandFunc {
	switch action {
	case protocol.ActionCreate:
		return createThing

	case protocol.ActionModify:
		return modifyThing

	case protocol.ActionDelete:
		return deleteThing

	case protocol.ActionRetrieve:
		return retrieveThing

	default:
		return nil
	}
}

func featuresPathCommand(action protocol.TopicAction, cmdType Scope) CommandFunc {
	switch cmdType {
	case ScopeFeatures:
		// /features
		return featuresCommand(action)

	case ScopeFeature:
		// /features/<featureID>
		return featureCommand(action)

	case ScopeFeatureProperties:
		// /features/<featureID>/properties
		return propertiesCommand(action)

	case ScopeFeatureProperty:
		// /features/<featureID>/properties/<propertyPath>
		return propertyCommand(action)

	case ScopeFeatureDesiredProperties:
		// /features/<featureID>/desiredProperties
		return desiredPropertiesCommand(action)

	case ScopeFeatureDesiredProperty:
		// /features/<featureID>/desiredProperties/<propertyPath>
		return desiredPropertyCommand(action)

	default:
		// /features/<featureID>/definition
		return nil // unsupported
	}
}

func featuresCommand(action protocol.TopicAction) CommandFunc {
	switch action {
	case protocol.ActionModify:
		return modifyFeatures

	case protocol.ActionDelete:
		return deleteFeatures

	case protocol.ActionRetrieve:
		return retrieveFeatures

	default:
		return nil
	}
}

func featureCommand(action protocol.TopicAction) CommandFunc {
	switch action {
	case protocol.ActionModify:
		return modifyFeature

	case protocol.ActionDelete:
		return deleteFeature

	case protocol.ActionRetrieve:
		return retrieveFeature

	default:
		return nil
	}
}

func propertiesCommand(action protocol.TopicAction) CommandFunc {
	switch action {
	case protocol.ActionModify:
		return modifyProperties

	case protocol.ActionDelete:
		return deleteProperties

	case protocol.ActionRetrieve:
		return retrieveProperties

	default:
		return nil
	}
}

func desiredPropertiesCommand(action protocol.TopicAction) CommandFunc {
	switch action {
	case protocol.ActionModify:
		return modifyDesiredProperties

	case protocol.ActionDelete:
		return deleteDesiredProperties

	case protocol.ActionRetrieve:
		return retrieveDesiredProperties

	default:
		return nil
	}
}

func propertyCommand(action protocol.TopicAction) CommandFunc {
	switch action {
	case protocol.ActionModify:
		return modifyProperty

	case protocol.ActionDelete:
		return deleteProperty

	case protocol.ActionRetrieve:
		return retrieveProperty

	default:
		return nil
	}
}

func desiredPropertyCommand(action protocol.TopicAction) CommandFunc {
	switch action {
	case protocol.ActionModify:
		return modifyDesiredProperty

	case protocol.ActionDelete:
		return deleteDesiredProperty

	case protocol.ActionRetrieve:
		return retrieveDesiredProperty

	default:
		return nil
	}
}

func (h *Handler) eventEnvelope(
	thingID string, cmdEnvelope *protocol.Envelope, action protocol.TopicAction,
) *protocol.Envelope {
	thing := model.Thing{}
	if err := h.Storage.GetThingData(thingID, &thing); err != nil {
		logCmdError("Failed to create event on command execution. Unknown thing",
			err, cmdEnvelope, h.Logger)
		return nil
	}
	return eventEnvelope(cmdEnvelope, thing.Revision, thing.Timestamp, action)
}

func (h *Handler) resourceNotFound(
	msg string, err error, cmd *protocol.Envelope, thingID string, featureID string,
) *protocol.Envelope {
	logCmdError(msg, err, cmd, h.Logger)

	if cmd.Headers.ResponseRequired() {
		if errors.Is(err, persistence.ErrThingNotFound) {
			return NewThingNotFoundError(cmd, thingID)
		}
		return NewFeatureNotFoundError(cmd, thingID, featureID)
	}
	return nil
}

func (h *Handler) thingNotFound(
	msg string, err error, cmd *protocol.Envelope, thingID string,
) *protocol.Envelope {
	logCmdError(msg, err, cmd, h.Logger)

	if cmd.Headers.ResponseRequired() {
		return NewThingNotFoundError(cmd, thingID)
	}
	return nil
}

// LoadThing loads a persisted thing if present.
// If there is no such thing and auto-provisioning is enabled
// the thing is created, persisted and a thing created event is published.
// Returns error if other case.
func (h *Handler) LoadThing(thingID string, envelope *protocol.Envelope) (*model.Thing, error) {
	thing := model.Thing{}
	if err := h.Storage.GetThing(thingID, &thing); err != nil {
		if h.AutoProvisioning && errors.Is(err, persistence.ErrThingNotFound) {
			thing, err = autoprovisionThing(h, envelope, thingID)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return &thing, nil
}

// LoadFeature loads the feature from the given thing if present.
// If there is no such thing and auto-provisioning is enabled
// the thing is created, persisted and a thing created event is published.
// Then tries to load the feature from the created thing.
// Returns error if the thing is not presenting and auto-provisioning is disabled
// or there is no such feature.
func (h *Handler) LoadFeature(
	thingID string, featureID string, envelope *protocol.Envelope,
) (*model.Feature, error) {
	feature := model.Feature{}
	err := h.Storage.GetFeature(thingID, featureID, &feature)
	if err != nil {
		if h.AutoProvisioning && errors.Is(err, persistence.ErrThingNotFound) {
			if _, err = autoprovisionThing(h, envelope, thingID); err != nil {
				return nil, err
			}
			return nil, featureNotFoundError(thingID, featureID)
		}
		return nil, err
	}

	return &feature, nil
}

func autoprovisionThing(h *Handler, cmd *protocol.Envelope, thingID string) (model.Thing, error) {
	thing := (&model.Thing{}).WithIDFrom(thingID)
	if _, err := h.Storage.AddThing(thing); err != nil {
		return *thing, err
	}
	publishEvent(h, eventThingCreatedEnvelope(cmd, protocol.ActionCreated, thing))
	return *thing, nil
}

func featureNotFoundError(thingID string, featureID string) error {
	return errors.Wrapf(persistence.ErrFeatureNotFound,
		"feature with ID '%s' on the thing with ID '%s' could not be loaded", featureID, thingID)
}

func cmdWithNoResponseRequired(msg *message.Message, command *protocol.Envelope) *message.Message {
	rspReqCommand := command
	rspHeaders := command.Headers.Clone()
	rspReqCommand.Headers = rspHeaders.WithResponseRequired(false)
	buf, err := json.Marshal(rspReqCommand)
	if err != nil {
		return msg
	}
	newMsg := msg.Copy()
	newMsg.Payload = buf
	newMsg.SetContext(msg.Context())
	return newMsg
}

func (h *Handler) publishCommandLocalOutput(msg *message.Message, command *protocol.Envelope, output *CommandOutput) {
	if output.response != nil {
		publishResponse(h, output.response)
	}

	if output.event != nil {
		publishEvent(h, output.event)
	}
}

func (h *Handler) resourceSynchronized(output *CommandOutput) {
	if len(output.thingID) <= 0 {
		return
	}

	if len(output.featureID) > 0 {
		if ok, _ := h.Storage.FeatureSynchronized(output.thingID, output.featureID, output.revision); ok {
			h.Logger.Tracef("Feature '%s' of thing '%s' is marked as synchronized", output.featureID, output.thingID)
		}

	} else {
		if ok, _ := h.Storage.ThingSynchronized(output.thingID, output.revision); ok {
			h.Logger.Tracef("Thing '%s' is marked as synchronized", output.thingID)
		}
	}
}
