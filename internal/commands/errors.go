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
	"fmt"

	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
)

// ThingError represents error within the "things" namespace
type ThingError struct {
	Status      int    `json:"status"`
	Error       string `json:"error"`
	Message     string `json:"message"`
	Description string `json:"description,omitempty"`
}

// NewThingNotFoundError creates thing not found error.
func NewThingNotFoundError(cmdEnvelope *protocol.Envelope, thingID string) *protocol.Envelope {
	thingsErr := &ThingError{
		Status:      404,
		Error:       "things:thing.notfound",
		Message:     fmt.Sprintf("The Thing with ID '%s' could not be found.", thingID),
		Description: "Check if the ID of your requested Thing was correct.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewThingConflictError creates thing conflict error.
func NewThingConflictError(cmdEnvelope *protocol.Envelope, thingID string) *protocol.Envelope {
	thingsErr := &ThingError{
		Status:      409,
		Error:       "things:thing.conflict",
		Message:     fmt.Sprintf("The Thing with ID '%s' already exists.", thingID),
		Description: "Choose another Thing ID.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewFeatureNotFoundError creates feature not found error.
func NewFeatureNotFoundError(cmdEnvelope *protocol.Envelope, thingID string, featureID string) *protocol.Envelope {
	thingsErr := &ThingError{
		Status: 404,
		Error:  "things:feature.notfound",
		Message: fmt.Sprintf(
			"The Feature with ID '%s' on the Thing with ID '%s' could not be found.",
			featureID, thingID),
		Description: "Check if the ID of the Thing and the ID of your requested Feature was correct.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewFeaturesNotFoundError creates features not found error.
func NewFeaturesNotFoundError(cmdEnvelope *protocol.Envelope, thingID string) *protocol.Envelope {
	thingsErr := &ThingError{
		Status:      404,
		Error:       "things:features.notfound",
		Message:     fmt.Sprintf("The Features on the Thing with ID '%s' do not exist.", thingID),
		Description: "Check if the ID of the Thing was correct.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewPropertiesNotFoundError creates properties not found error.
func NewPropertiesNotFoundError(
	cmdEnvelope *protocol.Envelope, thingID string, featureID string, desired bool,
) *protocol.Envelope {
	var err, msg string
	if desired {
		err = "things:feature.desiredProperties.notfound"
		msg = "The desired properties of the Feature with ID '%s' on the Thing with ID '%s' do not exist."
	} else {
		err = "things:feature.properties.notfound"
		msg = "The Properties of the Feature with ID '%s' on the Thing with ID '%s' do not exist."

	}
	thingsErr := &ThingError{
		Status:      404,
		Error:       err,
		Message:     fmt.Sprintf(msg, featureID, thingID),
		Description: "Check if the ID of the Thing and the Feature ID was correct.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewPropertyNotFoundError creates property not found error.
func NewPropertyNotFoundError(
	cmdEnvelope *protocol.Envelope, thingID string, featureID string, pointer string, desired bool,
) *protocol.Envelope {
	var err, msg string
	if desired {
		err = "things:feature.desiredProperty.notfound"
		msg = "The desired property"
	} else {
		err = "things:feature.property.notfound"
		msg = "The property"
	}
	thingsErr := &ThingError{
		Status: 404,
		Error:  err,
		Message: fmt.Sprintf(
			msg+" with JSON Pointer '%s' of the Feature with ID '%s' on the Thing with ID '%s' does not exist.",
			pointer,
			featureID,
			thingID,
		),
		Description: "Check if the ID of the Thing, the Feature ID and the key of your requested property was correct.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewIDNotSettableError creates Thing ID not settable error.
func NewIDNotSettableError(cmdEnvelope *protocol.Envelope) *protocol.Envelope {
	thingsErr := &ThingError{
		Status:      400,
		Error:       "things:id.notsettable",
		Message:     "The Thing ID in the command value is not equal to the Thing ID in the command topic.",
		Description: "Either delete the Thing ID from the command value or use the same Thing ID as in the command topic.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewIDInvalidError creates invalid Thing ID error.
func NewIDInvalidError(cmdEnvelope *protocol.Envelope, thingID string) *protocol.Envelope {
	thingsErr := &ThingError{
		Status:      400,
		Error:       "things:id.invalid",
		Message:     fmt.Sprintf("Thing ID '%s' is not valid!", thingID),
		Description: "It must conform to the namespaced entity ID notation (see Ditto documentation)",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewInvalidJSONValueError creates invalid json format error.
func NewInvalidJSONValueError(cmdEnvelope *protocol.Envelope, jsonError error) *protocol.Envelope {
	thingsErr := &ThingError{
		Status:      400,
		Error:       "json.invalid",
		Message:     fmt.Sprintf("Failed to parse command value: %s.", jsonError),
		Description: "Check if the JSON was valid and if it was in required format.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewInvalidFieldSelectorError creates invalid json field selector error.
func NewInvalidFieldSelectorError(cmdEnvelope *protocol.Envelope, jsonError error) *protocol.Envelope {
	thingsErr := &ThingError{
		Status:      400,
		Error:       "json.fieldselector.invalid",
		Message:     fmt.Sprintf("Invalid field selector: %s.", jsonError),
		Description: "Check fields syntax.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

// NewUnknownError creates ThingError for unexpected error.
func NewUnknownError(cmdEnvelope *protocol.Envelope, msg string, error error) *protocol.Envelope {
	thingsErr := &ThingError{
		Status:      400,
		Error:       "unknown",
		Message:     fmt.Sprintf("%s: %s.", msg, error),
		Description: "Unexpected error on command execution. Try it later.",
	}
	return errorEnvelope(cmdEnvelope, thingsErr)
}

func errorEnvelope(cmdEnvelope *protocol.Envelope, value *ThingError) *protocol.Envelope {
	env := &protocol.Envelope{
		Topic: &protocol.Topic{
			Namespace: cmdEnvelope.Topic.Namespace,
			EntityID:  cmdEnvelope.Topic.EntityID,
			Group:     protocol.GroupThings,
			Channel:   protocol.ChannelTwin,
			Criterion: protocol.CriterionErrors,
		},
		Headers: responseHeadersWithContent(cmdEnvelope.Headers),
		Path:    "/",
		Status:  value.Status,
	}
	return env.WithValue(value)
}
