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
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/pkg/errors"
)

var errorPropertiesNotFound = errors.New("properties of feature could not be found")
var errorDesiredPropertiesNotFound = errors.New("desired properties of feature could not be found")

// modifyProperties handles add/update properties commands and builds the command output.
func modifyProperties(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertiesModify(h, cmd, false, out)
}

// modifyDesiredProperties handles add/update desired properties commands and builds the command output.
func modifyDesiredProperties(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertiesModify(h, cmd, true, out)
}

// retrieveProperties handles retrieve properties commands and builds the command output.
func retrieveProperties(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertiesRetrieve(h, cmd, false, out)
}

// retrieveDesiredProperties handles retrieve desired properties commands and builds the command output.
func retrieveDesiredProperties(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertiesRetrieve(h, cmd, true, out)
}

// deleteProperties handles delete properties commands and builds the command output.
func deleteProperties(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertiesDelete(h, cmd, false, out)
}

// deleteDesiredProperties handles delete desired properties commands and builds the command output.
func deleteDesiredProperties(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertiesDelete(h, cmd, true, out)
}

func doPropertiesModify(h *Handler, cmd *Command, desired bool, out *CommandOutput) {
	thingID := cmd.thingID
	featureID := cmd.target

	if feature, err := h.LoadFeature(thingID, featureID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Modify feature's properties failed. Unknown feature",
			err, cmd.envelope, thingID, featureID)
	} else {
		var newValue map[string]interface{}
		if err := commandValue(cmd.envelope, &newValue, out); err == nil {
			status := modified
			action := protocol.ActionModified
			if desired {
				if feature.DesiredProperties == nil {
					status = created
					action = protocol.ActionCreated
				}
				feature.WithDesiredProperties(newValue)
			} else {
				if feature.Properties == nil {
					status = created
					action = protocol.ActionCreated
				}
				feature.WithProperties(newValue)
			}

			if rev, err := h.Storage.AddFeature(thingID, featureID, feature); err != nil {
				out.response = commandUnknownError("Update feature's properties failed", err, cmd.envelope, h.Logger)
			} else {
				out.response = responseEnvelope(cmd.envelope, status)
				out.event = h.eventEnvelope(thingID, cmd.envelope, action)

				out.thingID = thingID
				out.featureID = featureID
				out.revision = rev
			}
		}
	}
}

func doPropertiesRetrieve(h *Handler, cmd *Command, desired bool, out *CommandOutput) {
	thingID := cmd.thingID
	featureID := cmd.target

	if feature, err := h.LoadFeature(thingID, featureID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Unable to retrieve properties. Feature not found",
			err, cmd.envelope, thingID, featureID)
	} else {
		var properties map[string]interface{}
		if desired {
			properties = feature.DesiredProperties
		} else {
			properties = feature.Properties
		}

		if properties == nil {
			out.response = h.propertiesNotFound("Unable to retrieve properties of feature ID "+featureID,
				cmd.envelope, thingID, featureID, desired)
		} else {
			out.response = ResponseEnvelopeWithValue(cmd.envelope, ok, properties)
		}
	}
}

func doPropertiesDelete(h *Handler, cmd *Command, desired bool, out *CommandOutput) {
	thingID := cmd.thingID
	featureID := cmd.target

	if feature, err := h.LoadFeature(thingID, featureID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Delete feature's properties failed. Feature not found",
			err, cmd.envelope, thingID, featureID)

	} else {
		if desired {
			if feature.DesiredProperties == nil {
				out.response = h.propertiesNotFound("Delete desired properties failed for feature ID "+featureID,
					cmd.envelope, thingID, featureID, true)
				return
			}
			feature.WithDesiredProperties(nil)
		} else {
			if feature.Properties == nil {
				out.response = h.propertiesNotFound("Delete properties failed for feature ID "+featureID,
					cmd.envelope, thingID, featureID, false)
				return
			}
			feature.WithProperties(nil)
		}

		if rev, err := h.Storage.AddFeature(thingID, featureID, feature); err != nil {
			out.response = commandUnknownError("Delete feature's properties failed", err, cmd.envelope, h.Logger)
		} else {
			out.response = responseEnvelope(cmd.envelope, deleted)
			out.event = h.eventEnvelope(thingID, cmd.envelope, protocol.ActionDeleted)
			if !desired || rev == 1 {
				out.thingID = thingID
				out.featureID = featureID
				out.revision = rev
			}
		}
	}
}

func (h *Handler) propertiesNotFound(msg string, cmd *protocol.Envelope,
	thingID string, featureID string, desired bool,
) *protocol.Envelope {
	if desired {
		logCmdError(msg, errorDesiredPropertiesNotFound, cmd, h.Logger)
	} else {
		logCmdError(msg, errorPropertiesNotFound, cmd, h.Logger)
	}

	if cmd.Headers.ResponseRequired() {
		return NewPropertiesNotFoundError(cmd, thingID, featureID, desired)
	}
	return nil
}
