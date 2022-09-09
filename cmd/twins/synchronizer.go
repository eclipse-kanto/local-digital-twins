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

package main

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/eclipse-kanto/local-digital-twins/internal/sync"
	"github.com/eclipse-kanto/suite-connector/logger"
)

type synchronizeHandler struct {
	logger       logger.Logger
	synchronizer *sync.Synchronizer
}

func (h *synchronizeHandler) Connected(connected bool, err error) {
	if connected {
		go func() {
			time.Sleep(2 * time.Second)
			if err := h.synchronizer.Start(); err != nil {
				h.logger.Error("Synchronize error", err, nil)
			}
		}()
	} else {
		go h.synchronizer.Stop()
	}
}

func syncMiddleware(logger watermill.LoggerAdapter, synchronizer *sync.Synchronizer) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(message *message.Message) ([]*message.Message, error) {
			msgs, err := synchronizer.HandleResponse(message)
			if err != nil {
				logger.Error("Hub response message consumed by local synchronizer with error", err, nil)
				return nil, err
			}

			if msgs == nil {
				logger.Trace("Hub response message consumed by local synchronizer", nil)
				return nil, nil
			}

			return h(message)
		}
	}
}
