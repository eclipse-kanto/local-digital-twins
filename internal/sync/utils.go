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

package sync

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/eclipse-kanto/local-digital-twins/internal/commands"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/suite-connector/logger"
)

func publishHonoMsg(env *protocol.Envelope, publisher message.Publisher,
	dInfo commands.DeviceInfo, thingID string, logger logger.Logger) error {
	data, err := json.Marshal(env)

	if err != nil {
		logger.Error("Unexpected synchronize command content", err, commands.CmdLogFields(env))
		return err
	}

	message := message.NewMessage(watermill.NewUUID(), []byte(data))
	return commands.PublishHonoMsg(message, publisher, dInfo, thingID)
}

func logFieldsFeature(thingID string, featureID string) watermill.LogFields {
	return watermill.LogFields{
		"thing":   thingID,
		"feature": featureID,
	}
}

func logFeatureSynchronized(thingID string, featureID string, ok bool) watermill.LogFields {
	return watermill.LogFields{
		"synchronized": ok,
		"thing":        thingID,
		"feature":      featureID,
	}
}

func logFeatureError(thingID string, featureID string, err error) watermill.LogFields {
	return watermill.LogFields{
		"error":   err,
		"thing":   thingID,
		"feature": featureID,
	}
}
