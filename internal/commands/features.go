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
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/pkg/errors"
)

var errorFeaturesNotFound = errors.New("features could not be found")

// modifyFeatures handles add/update features commands and builds the command output.
func modifyFeatures(h *Handler, cmd *Command, out *CommandOutput) {
	features := map[string]*model.Feature{}
	if err := commandValue(cmd.envelope, &features, out); err == nil {
		thingID := cmd.thingID
		if thing, err := h.LoadThing(thingID, cmd.envelope); err != nil {
			out.response = h.resourceNotFound("Modify thing features failed. Unknown thing",
				err, cmd.envelope, thingID, noValue)
		} else {
			status := modified
			action := protocol.ActionModified
			if thing.Features == nil {
				status = created
				action = protocol.ActionCreated
			}

			thing.WithFeatures(features)
			if rev, err := h.Storage.AddThing(thing); err != nil {
				out.response = commandUnknownError("Modify thing features failed", err, cmd.envelope, h.Logger)

			} else {
				out.response = responseEnvelope(cmd.envelope, status)
				out.event = h.eventEnvelope(thingID, cmd.envelope, action)
				out.thingID = thingID
				out.revision = rev
			}
		}
	}
}

// retrieveFeatures handles retrieve features commands and builds the command output.
func retrieveFeatures(h *Handler, cmd *Command, out *CommandOutput) {
	thingID := cmd.thingID

	if thing, err := h.LoadThing(thingID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Retrieve thing features failed. Unknown thing",
			err, cmd.envelope, thingID, noValue)
	} else {
		if thing.Features == nil {
			out.response = h.featuresNotFound("Unable to retrieve any features of thing ID "+thingID,
				cmd.envelope, thingID)
		} else {
			out.response = ResponseEnvelopeWithValue(cmd.envelope, ok, thing.Features)
		}
	}
}

// deleteFeatures handles delete features commands and builds the command output.
func deleteFeatures(h *Handler, cmd *Command, out *CommandOutput) {
	thingID := cmd.thingID

	if thing, err := h.LoadThing(thingID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Delete thing features failed. Unknown thing",
			err, cmd.envelope, thingID, noValue)
	} else {
		if thing.Features == nil {
			out.response = h.featuresNotFound("Unable to delete any features of thing ID "+thingID,
				cmd.envelope, thingID)

		} else {
			thing.WithFeatures(nil)
			if rev, err := h.Storage.AddThing(thing); err != nil {
				out.response = commandUnknownError("Delete thing features failed. Unknown thing.",
					err, cmd.envelope, h.Logger)

			} else {
				out.response = responseEnvelope(cmd.envelope, deleted)
				out.event = h.eventEnvelope(thingID, cmd.envelope, protocol.ActionDeleted)
				out.thingID = thingID
				out.revision = rev
			}
		}
	}
}

func (h *Handler) featuresNotFound(msg string, cmd *protocol.Envelope, thingID string) *protocol.Envelope {
	logCmdError(msg, errorFeaturesNotFound, cmd, h.Logger)

	if cmd.Headers.ResponseRequired() {
		return NewFeaturesNotFoundError(cmd, thingID)
	}
	return nil
}
