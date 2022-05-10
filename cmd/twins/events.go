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

package main

import (
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/eclipse-kanto/local-digital-twins/internal/commands"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/suite-connector/logger"

	conn "github.com/eclipse-kanto/suite-connector/connector"
)

const (
	topicsEvent = "event/#,e/#"
)

func eventsBus(router *message.Router,
	honoPub message.Publisher,
	mosquittoClient *conn.MQTTConnection,
	deviceInfo commands.DeviceInfo,
	storage persistence.ThingsStorage,
	logger logger.Logger,
) *message.Handler {
	mosquittoSub := conn.NewSubscriber(mosquittoClient, conn.QosAtLeastOnce, false, router.Logger(), nil)

	h := &commands.Handler{
		DeviceInfo:   deviceInfo,
		MosquittoPub: conn.NewPublisher(mosquittoClient, conn.QosAtLeastOnce, router.Logger(), nil),
		HonoPub:      honoPub,
		Storage:      storage,
		Logger:       logger,
	}

	//Gateway -> Mosquitto Broker -> Message bus -> Hono
	return router.AddHandler("events_bus",
		topicsEvent,
		mosquittoSub,
		conn.TopicEmpty,
		honoPub,
		h.HandleCommand,
	)
}
