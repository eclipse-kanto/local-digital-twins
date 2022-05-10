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
	parser "github.com/Jeffail/gabs/v2"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/pkg/errors"
)

var errorPropertyNotFound = errors.New("property of feature could not be found")
var errorDesiredPropertyNotFound = errors.New("desired property of feature could not be found")

// modifyProperty handles add/update property commands and builds the command output.
func modifyProperty(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertyModify(h, cmd, false, out)
}

// modifyDesiredProperty handles add/update desired property commands and builds the command output.
func modifyDesiredProperty(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertyModify(h, cmd, true, out)
}

// retrieveProperty handles retrieve property commands and builds the command output.
func retrieveProperty(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertyRetrieve(h, cmd, false, out)
}

// retrieveDesiredProperty handles retrieve desired property commands and builds the command output.
func retrieveDesiredProperty(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertyRetrieve(h, cmd, true, out)
}

// deleteProperty handles delete property commands and builds the command output.
func deleteProperty(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertyDelete(h, cmd, false, out)
}

// deleteDesiredProperty handles delete desired property commands and builds the command output.
func deleteDesiredProperty(h *Handler, cmd *Command, out *CommandOutput) {
	doPropertyDelete(h, cmd, true, out)
}

func doPropertyModify(h *Handler, cmd *Command, desired bool, out *CommandOutput) {
	thingID := cmd.thingID
	featureID := cmd.target

	if feature, err := h.LoadFeature(thingID, featureID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Modify feature property failed. Feature not found",
			err, cmd.envelope, thingID, featureID)
	} else {
		var propertiesContainer *parser.Container
		status := modified
		action := protocol.ActionModified
		if desired {
			if feature.DesiredProperties == nil {
				status = created
				action = protocol.ActionCreated
				feature.DesiredProperties = make(map[string]interface{})
			}
			propertiesContainer = parser.Wrap(feature.DesiredProperties)
		} else {
			if feature.Properties == nil {
				status = created
				action = protocol.ActionCreated
				feature.Properties = make(map[string]interface{})
			}
			propertiesContainer = parser.Wrap(feature.Properties)
		}

		var newValue interface{}
		if err := commandValue(cmd.envelope, &newValue, out); err == nil {
			if _, err := propertiesContainer.SetJSONPointer(newValue, cmd.path); err != nil {
				out.response = commandPropertyNotFoundError("Update feature property failed. Unable to set pointer value",
					err, cmd, desired, h.Logger)
				return
			}

			if rev, err := h.Storage.AddFeature(thingID, cmd.target, feature); err != nil {
				out.response = commandUnknownError("Update feature property failed", err, cmd.envelope, h.Logger)
			} else {
				out.response = responseEnvelope(cmd.envelope, status)
				out.event = h.eventEnvelope(thingID, cmd.envelope, action)
				addChangeInfo(out, thingID, featureID, rev)
			}
		}
	}
}

func doPropertyRetrieve(h *Handler, cmd *Command, desired bool, out *CommandOutput) {
	thingID := cmd.thingID
	featureID := cmd.target

	if feature, err := h.LoadFeature(thingID, featureID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Unable to retrieve feature property. Feature not found",
			err, cmd.envelope, thingID, featureID)
	} else {
		var properties map[string]interface{}
		var propErr error
		if desired {
			properties = feature.DesiredProperties
			propErr = errorDesiredPropertyNotFound
		} else {
			properties = feature.Properties
			propErr = errorPropertyNotFound
		}

		if properties == nil {
			out.response = commandPropertyNotFoundError("Unable to retrieve any properties of feature "+cmd.target,
				propErr, cmd, desired, h.Logger)
			return
		}

		propertiesContainer := parser.Wrap(properties)
		if propValue, err := propertiesContainer.JSONPointer(cmd.path); err != nil {
			out.response = commandPropertyNotFoundError("Unable to retrieve property path "+cmd.path,
				propErr, cmd, desired, h.Logger)
		} else {
			out.response = ResponseEnvelopeWithValue(cmd.envelope, ok, propValue.Data())
		}
	}
}

func doPropertyDelete(h *Handler, cmd *Command, desired bool, out *CommandOutput) {
	thingID := cmd.thingID
	featureID := cmd.target

	if feature, err := h.LoadFeature(thingID, featureID, cmd.envelope); err != nil {
		out.response = h.resourceNotFound("Delete feature property failed. Feature not found",
			err, cmd.envelope, thingID, featureID)
	} else {
		var properties map[string]interface{}
		if desired {
			properties = feature.DesiredProperties
		} else {
			properties = feature.Properties
		}

		if properties == nil {
			out.response = commandPropertyNotFoundError("Delete feature property failed",
				errors.New("no properties"), cmd, desired, h.Logger)
			return
		}

		if pathSlice, err := parser.JSONPointerToSlice(cmd.path); err != nil {
			out.response = commandPropertyNotFoundError("Delete feature property failed. Invalid path.",
				err, cmd, desired, h.Logger)
		} else {
			deletePropertyPathSlice(h, cmd, feature, properties, pathSlice, desired, out)
		}
	}
}

func deletePropertyPathSlice(h *Handler, cmd *Command, feature *model.Feature,
	properties map[string]interface{}, pathSlice []string, desired bool, out *CommandOutput) {
	propertiesContainer := parser.Wrap(properties)
	thingID := cmd.thingID
	featureID := cmd.target
	if err := propertiesContainer.Delete(pathSlice...); err != nil {
		out.response = commandPropertyNotFoundError("Delete feature property path failed", err, cmd, desired, h.Logger)
		return
	}

	if len(properties) == 0 {
		if desired {
			feature.WithDesiredProperties(nil)
		} else {
			feature.WithProperties(nil)
		}
	}

	if rev, err := h.Storage.AddFeature(thingID, featureID, feature); err != nil {
		out.response = commandPropertyNotFoundError("Delete feature property failed", err, cmd, desired, h.Logger)

	} else {
		out.response = responseEnvelope(cmd.envelope, deleted)
		out.event = h.eventEnvelope(thingID, cmd.envelope, protocol.ActionDeleted)
		addChangeInfo(out, thingID, featureID, rev)
	}
}

func addChangeInfo(out *CommandOutput, thingID, featureID string, revision int64) {
	if revision == 1 {
		// no other unsynchronized feature changes
		out.thingID = thingID
		out.featureID = featureID
		out.revision = 1
	}
}
