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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTwinSettingsDefaults(t *testing.T) {
	settings := DefaultSettings()
	assert.NotNil(t, settings.LocalConnection())
	assert.NotNil(t, settings.HubConnection())
	assert.Equal(t, settings.ProvisioningFile, settings.Provisioning())
	assert.Equal(t, settings, settings.DeepCopy())
}

func TestParamsAnnounceTimeout(t *testing.T) {
	timeout := os.Getenv("HUB_PARAMS_ANNOUNCE_TIMEOUT")

	defer func() {
		assert.NoError(t, os.Setenv("HUB_PARAMS_ANNOUNCE_TIMEOUT", timeout))
	}()

	assert.NoError(t, os.Setenv("HUB_PARAMS_ANNOUNCE_TIMEOUT", "1"))
	assert.Equal(t, time.Second, hubParamsAnnounceTimeout())
}
