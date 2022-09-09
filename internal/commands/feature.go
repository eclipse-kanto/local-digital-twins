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
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/pkg/errors"
)

// modifyFeature handles add/update feature commands and builds the command output.
func modifyFeature(h *Handler, cmd *Command, out *CommandOutput) {
	env := cmd.envelope

	feature := model.Feature{}
	if err := commandValue(env, &feature, out); err == nil {
		thingID := cmd.thingID
		featureID := cmd.target

		status := modified
		action := protocol.ActionModified
		if err := h.Storage.GetFeature(thingID, featureID, &model.Feature{}); err != nil {
			if _, err := h.LoadThing(thingID, env); err != nil {
				out.response = h.thingNotFound("Modify feature failed", err, env, thingID)
				return
			}
			status = created
			action = protocol.ActionCreated
		}

		if rev, err := h.Storage.AddFeature(thingID, featureID, &feature); err != nil {
			out.response = h.resourceNotFound("Modify feature failed", err, env, thingID, featureID)
		} else {
			out.response = responseEnvelope(env, status)
			out.event = h.eventEnvelope(thingID, env, action)

			out.thingID = thingID
			out.featureID = featureID
			out.revision = rev
		}
	}
}

// retrieveFeature handles retrieve feature commands and builds the command output.
func retrieveFeature(h *Handler, cmd *Command, out *CommandOutput) {
	thingID := cmd.thingID
	featureID := cmd.target

	if feature, err := h.LoadFeature(thingID, featureID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Unable to retrieve feature. Feature not found",
			err, cmd.envelope, thingID, featureID)
	} else {
		out.response = ResponseEnvelopeWithValue(cmd.envelope, ok, feature)
	}
}

// deleteFeature handles delete feature commands and builds the command output.
func deleteFeature(h *Handler, cmd *Command, out *CommandOutput) {
	thingID := cmd.thingID
	featureID := cmd.target

	if err := h.Storage.RemoveFeature(thingID, featureID); err != nil {
		if h.AutoProvisioning && errors.Is(err, persistence.ErrThingNotFound) {
			if _, err = autoprovisionThing(h, cmd.envelope, thingID); err == nil {
				err = featureNotFoundError(thingID, featureID)
			}
		}
		out.response = h.resourceNotFound("Delete feature failed",
			err, cmd.envelope, thingID, featureID)
	} else {
		out.response = responseEnvelope(cmd.envelope, deleted)
		out.event = h.eventEnvelope(thingID, cmd.envelope, protocol.ActionDeleted)
		out.thingID = thingID
		out.featureID = featureID
	}
}
