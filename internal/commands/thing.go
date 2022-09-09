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

package commands

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/eclipse-kanto/local-digital-twins/internal/jsonutil"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/pkg/errors"
)

const (
	thingIDs = "thingIds"
)

// createThing handles create thing commands and builds the command output.
func createThing(h *Handler, cmd *Command, out *CommandOutput) {
	thing := commandThing(cmd.thingID, cmd.envelope, out)
	if thing != nil {
		thingData := model.Thing{}
		if err := h.Storage.GetThingData(cmd.thingID, &thingData); err != nil {
			performModifyThing(h, cmd.envelope, thing, created, protocol.ActionCreated, out)
		} else {
			out.response = h.conflictError("Create thing failed. Thing exists",
				err, cmd.envelope, cmd.thingID, noValue)
		}
	}
}

// modifyThing handles update thing commands and builds the command output.
func modifyThing(h *Handler, cmd *Command, out *CommandOutput) {
	thing := commandThing(cmd.thingID, cmd.envelope, out)
	if thing != nil {
		status := modified
		if err := h.Storage.GetThingData(cmd.thingID, &model.Thing{}); err != nil {
			status = created
		}
		performModifyThing(h, cmd.envelope, thing, status, protocol.ActionModified, out)
	}
}

// retrieveThing handles retrieve a thing or list of things when multiple thing IDs provided
// commands and builds the command output.
func retrieveThing(h *Handler, cmd *Command, out *CommandOutput) {
	if cmd.envelope.Topic.Namespace == protocol.TopicPlaceholder ||
		cmd.envelope.Topic.EntityID == protocol.TopicPlaceholder {
		retrieveThings(h, cmd, out)
	} else {
		thing := model.Thing{}

		if err := h.Storage.GetThing(cmd.thingID, &thing); err != nil {
			out.response = h.thingNotFound("Retrieve thing failed", err, cmd.envelope, cmd.thingID)
			return
		}

		if len(cmd.envelope.Fields) == 0 {
			out.response = ResponseEnvelopeWithValue(cmd.envelope, ok, thing)
		} else {
			out.response = h.responseEnvelopeWithFields(cmd.envelope, thing)
		}
	}
}

// retrieveThings handles retrieve multiple things commands and builds the command output.
func retrieveThings(h *Handler, cmd *Command, out *CommandOutput) {
	var cmdValue map[string][]string
	if err := json.Unmarshal(cmd.envelope.Value, &cmdValue); err != nil {
		out.response = NewInvalidJSONValueError(cmd.envelope, err)
	} else {
		thingIds := cmdValue[thingIDs]
		if len(thingIds) == 0 {
			out.response = NewInvalidJSONValueError(cmd.envelope,
				errors.New(fmt.Sprintf("Empty '%s' value", thingIDs)))
		} else {
			out.response = doRetrieveThings(h, cmd.envelope, thingIds)
		}
	}
}

// deleteThing handles delete thing commands and builds the command output.
func deleteThing(h *Handler, cmd *Command, out *CommandOutput) {
	if thing, err := h.LoadThing(cmd.thingID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Delete thing failed. Unknown thing",
			err, cmd.envelope, cmd.thingID, noValue)

	} else {
		out.response = responseEnvelope(cmd.envelope, deleted)
		out.event = eventEnvelope(cmd.envelope, thing.Revision, thing.Timestamp, protocol.ActionDeleted)
	}
}

func commandThing(thingID string, env *protocol.Envelope, out *CommandOutput) *model.Thing {
	thing := model.Thing{}
	if err := commandValue(env, &thing, out); err != nil {
		return nil
	}

	if thing.ID == nil {
		thing.WithIDFrom(thingID)

	} else {
		if thing.ID.String() != thingID {
			out.response = NewIDNotSettableError(env)
			return nil
		}
	}

	return &thing
}

func performModifyThing(h *Handler, env *protocol.Envelope, thing *model.Thing,
	status int, action protocol.TopicAction, out *CommandOutput) {
	if rev, err := h.Storage.AddThing(thing); err != nil {
		out.response = commandUnknownError("Modify thing failed", err, env, h.Logger)
	} else {
		if status == created {
			out.response = ResponseEnvelopeWithValue(env, status, thing)
		} else {
			out.response = responseEnvelope(env, status)
		}

		out.event = eventThingCreatedEnvelope(env, action, thing)

		out.thingID = thing.ID.String()
		out.revision = rev
	}
}

func doRetrieveThings(h *Handler, env *protocol.Envelope, thingIds []string) *protocol.Envelope {
	thingsArray := make([]model.Thing, 0)
	for _, thingID := range thingIds {
		if !strings.Contains(thingID, ":") {
			return NewIDInvalidError(env, thingID)
		}
		thing := model.Thing{}
		if err := h.Storage.GetThing(thingID, &thing); err == nil {
			thingsArray = append(thingsArray, thing)
		}
	}

	if len(env.Fields) == 0 {
		return ResponseEnvelopeWithValue(env, ok, thingsArray)
	}

	return h.responseEnvelopeWithFieldsArr(env, thingsArray)
}

func (h *Handler) conflictError(msg string, err error, env *protocol.Envelope, thingID string, featureID string,
) *protocol.Envelope {
	logCmdError(msg, err, env, h.Logger)

	if env.Headers.ResponseRequired() {
		return NewThingConflictError(env, thingID)
	}
	return nil
}

func (h *Handler) invalidFieldSelector(msg string, err error, env *protocol.Envelope) *protocol.Envelope {
	logCmdError(msg, err, env, h.Logger)

	if env.Headers.ResponseRequired() {
		return NewInvalidFieldSelectorError(env, err)
	}
	return nil
}

func (h *Handler) responseEnvelopeWithFields(env *protocol.Envelope, thing model.Thing) *protocol.Envelope {
	thingByte, err := json.Marshal(thing)
	if err != nil {
		return commandUnknownError("Thing marshal error", err, env, h.Logger)
	}

	str, err := jsonutil.JSONSubset(string(thingByte), env.Fields)
	if err != nil {
		return h.invalidFieldSelector("Invalid field selector", err, env)
	}

	fieldsThing := model.Thing{}
	if err := json.Unmarshal([]byte(str), &fieldsThing); err != nil {
		return commandUnknownError("Thing unmarshal error", err, env, h.Logger)
	}
	return ResponseEnvelopeWithValue(env, ok, fieldsThing)
}

func (h *Handler) responseEnvelopeWithFieldsArr(env *protocol.Envelope, things []model.Thing) *protocol.Envelope {
	marshalArray, err := json.Marshal(things)
	if err != nil {
		return commandUnknownError("Things array marshal error", err, env, h.Logger)
	}

	subArray, err := jsonutil.JSONArraySubset(string(marshalArray), env.Fields)
	if err != nil {
		return h.invalidFieldSelector("Invalid field selector", err, env)
	}

	subThingsArray := make([]model.Thing, 0)
	if err := json.Unmarshal([]byte(subArray), &subThingsArray); err != nil {
		return commandUnknownError("Things array unmarshal error", err, env, h.Logger)
	}
	return ResponseEnvelopeWithValue(env, ok, subThingsArray)
}
