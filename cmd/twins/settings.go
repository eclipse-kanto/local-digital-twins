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
	"os"
	"strconv"
	"time"

	"github.com/eclipse-kanto/suite-connector/config"
)

// TwinSettings contains the Local Digital Twin configurable data.
type TwinSettings struct {
	config.Settings

	ThingsDb string `json:"thingsDb"`
}

// Provisioning implementation.
func (settings *TwinSettings) Provisioning() string {
	return settings.ProvisioningFile
}

// LocalConnection implementation.
func (settings *TwinSettings) LocalConnection() *config.LocalConnectionSettings {
	return &settings.LocalConnectionSettings
}

// HubConnection implementation.
func (settings *TwinSettings) HubConnection() *config.HubConnectionSettings {
	return &settings.HubConnectionSettings
}

// DeepCopy implementation.
func (settings *TwinSettings) DeepCopy() config.SettingsAccessor {
	clone := *settings
	return &clone
}

// DefaultSettings returns the default settings.
func DefaultSettings() *TwinSettings {
	def := config.DefaultSettings()
	def.LogFile = "log/local-digital-twins.log"

	return &TwinSettings{
		Settings: *def,
		ThingsDb: "things.db",
	}
}

func hubParamsAnnounceTimeout() time.Duration {
	timeout := 5 * time.Second
	if t, err := strconv.ParseInt(os.Getenv("HUB_PARAMS_ANNOUNCE_TIMEOUT"), 0, 64); err == nil {
		if t > 0 {
			timeout = time.Duration(t) * time.Second
		}
	}
	return timeout
}
