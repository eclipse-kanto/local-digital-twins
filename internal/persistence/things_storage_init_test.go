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

package persistence_test

import (
	"errors"
	"os"
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitStorageOnDeviceChange(t *testing.T) {
	deviceID := "org.eclipse.kanto:TestInitStorageSameDevice"
	dbDir := "test"
	location := dbDir + "/TestInitStorageSameDevice.db"

	os.Mkdir(dbDir, 0700)
	require.DirExists(t, dbDir)

	defer func() {
		err := os.RemoveAll(dbDir)
		require.NoError(t, err, "Error on %s db test folder removal %s", location, err)
	}()

	entries, err := os.ReadDir(dbDir)
	require.NoError(t, err)
	require.Equal(t, 0, len(entries))

	db := assertDBDevice(t, location, deviceID)

	thingID := "org.eclipse.kanto:testThing"
	rev, err := db.AddThing((&model.Thing{}).WithIDFrom(thingID))
	require.NoError(t, err)
	require.NoError(t, db.Close())
	assert.Equal(t, int64(0), rev)

	db = assertDBDevice(t, location, deviceID)
	assertThing(t, db, thingID, true)
	require.NoError(t, db.Close())

	newDeviceID := deviceID + "_new"
	db = assertDBDevice(t, location, newDeviceID)
	assertThing(t, db, thingID, false)
	require.NoError(t, db.Close())

	db = assertDBDevice(t, location, deviceID)
	assertThing(t, db, thingID, false)
	require.NoError(t, db.Close())

	// assert expected db files
	entries, _ = os.ReadDir(dbDir)
	assert.Equal(t, 3, len(entries), "current", deviceID, newDeviceID)
}

func assertDBDevice(t *testing.T, path string, deviceID string) persistence.ThingsStorage {
	db, err := persistence.NewThingsDB(path, deviceID)
	require.NoError(t, err)
	assert.Equal(t, deviceID, db.GetDeviceID())
	return db
}

func assertThing(t *testing.T, db persistence.ThingsStorage, thingID string, present bool) {
	thingLoaded := &model.Thing{}
	err := db.GetThing(thingID, thingLoaded)

	if present {
		assert.NoError(t, err)
		assert.Equal(t, thingID, thingLoaded.ID.String())
	} else {
		assert.True(t, errors.Is(err, persistence.ErrThingNotFound), thingID, err)
	}
}
