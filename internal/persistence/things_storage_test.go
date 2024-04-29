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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence/data"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	dbLocation  = "tmp/things_storage_test.db"
	testThingID = "things.storage:test"

	testFeatureID1 = "TestFeature1"
	testFeatureID2 = "TestFeature2"
)

type PersistenceTestSuite struct {
	suite.Suite
	storage persistence.ThingsStorage
}

func TestPersistenceTestSuite(t *testing.T) {
	suite.Run(t, new(PersistenceTestSuite))
}

func (s *PersistenceTestSuite) SetupSuite() {
	db, err := persistence.NewThingsDB(dbLocation, testThingID)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), testThingID, db.GetDeviceID())
	s.storage = db
}

func (s *PersistenceTestSuite) TearDownSuite() {
	if s.storage != nil {
		err := s.storage.Close()
		assert.NoError(s.T(), err)
	}

	defer func() {
		if err := os.RemoveAll(filepath.Dir(dbLocation)); err != nil {
			fmt.Printf("Error on %s db test file removal %s", dbLocation, err)
		}
	}()

	if s.storage != nil {
		s.assertErrorsDatabaseClosed()
	}
}

func (s *PersistenceTestSuite) TearDownTest() {
	s.storage.RemoveThing(testThingID)
}

func (s *PersistenceTestSuite) assertErrorsDatabaseClosed() {
	_, err := s.storage.GetThingIDs()
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	thingID := "test:unknown"
	thing := (&model.Thing{}).WithID(model.NewNamespacedIDFrom(thingID))
	_, err = s.storage.AddThing(thing)
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	err = s.storage.GetThing(thingID, thing)
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	err = s.storage.RemoveThing(thingID)
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	err = s.storage.GetThingData(thingID, thing)
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	_, err = s.storage.GetSystemThingData(thingID)
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	ok, err := s.storage.ThingSynchronized(thingID, 1)
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)
	assert.False(s.T(), ok)

	featureID := "test"
	_, err = s.storage.AddFeature(thingID, featureID, &model.Feature{})
	require.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	err = s.storage.GetFeature(thingID, featureID, &model.Feature{})
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	err = s.storage.RemoveFeature(thingID, featureID)
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)

	ok, err = s.storage.FeatureSynchronized(thingID, featureID, 1)
	assert.True(s.T(), errors.Is(err, persistence.ErrDatabaseClosed), err)
	assert.False(s.T(), ok)
}

func (s *PersistenceTestSuite) TestThingNotFound() {
	thingID := "unknown"
	err := s.storage.GetThing(thingID, &model.Thing{})
	assert.True(s.T(), errors.Is(err, persistence.ErrThingNotFound), err)
	assert.True(s.T(), errors.Is(err, persistence.ErrNotFound), err)

	err = s.storage.RemoveThing(thingID)
	assert.True(s.T(), errors.Is(err, persistence.ErrThingNotFound), err)

	featureID := "test"
	_, err = s.storage.AddFeature(thingID, featureID, &model.Feature{})
	assert.True(s.T(), errors.Is(err, persistence.ErrThingNotFound), err)

	err = s.storage.GetFeature(thingID, featureID, &model.Feature{})
	assert.True(s.T(), errors.Is(err, persistence.ErrThingNotFound), err)

	err = s.storage.RemoveFeature(thingID, featureID)
	assert.True(s.T(), errors.Is(err, persistence.ErrThingNotFound), err)
}

func (s *PersistenceTestSuite) TestFeatureNotFound() {
	thingID := testThingID

	s.addThing(thingID, nil)

	featureID := "unknown"
	err := s.storage.GetFeature(testThingID, featureID, &model.Feature{})
	require.True(s.T(), errors.Is(err, persistence.ErrFeatureNotFound), err)
	require.True(s.T(), errors.Is(err, persistence.ErrNotFound), err)

	err = s.storage.RemoveFeature(testThingID, featureID)
	require.True(s.T(), errors.Is(err, persistence.ErrFeatureNotFound), err)
	require.True(s.T(), errors.Is(err, persistence.ErrNotFound), err)
}

func (s *PersistenceTestSuite) TestGetThingIDs() {
	ids, err := s.storage.GetThingIDs()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 0, len(ids), ids)

	thing := createThing(testThingID)

	// add thing
	rev, err := s.storage.AddThing(thing)
	require.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), rev, int64(0))

	ids, err = s.storage.GetThingIDs()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(ids), ids)
}

func (s *PersistenceTestSuite) TestAddThing() {
	thingID := testThingID + "_TestAddThing"
	thing := createThing(thingID)

	// add thing
	_, err := s.storage.AddThing(thing)
	require.NoError(s.T(), err)
	defer s.deleteTestThing(thingID)

	ids, err := s.storage.GetThingIDs()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(ids), ids)

	thingLoaded := &model.Thing{}
	err = s.storage.GetThing(thingID, thingLoaded)
	require.NoError(s.T(), err)
	assert.EqualValues(s.T(), thing.ID, thingLoaded.ID)
	assert.EqualValues(s.T(), thing.Features, thingLoaded.Features)
	assert.EqualValues(s.T(), thing.Revision, thingLoaded.Revision)

	// update thing
	newFeatureID := "newFeature"
	thing.WithFeatures(map[string]*model.Feature{
		newFeatureID: {
			Properties: map[string]interface{}{"prop1": "prop1Val"},
		}})

	_, err = s.storage.AddThing(thing)
	require.NoError(s.T(), err)
	s.assertFeatureSynchState(thingID, newFeatureID, false)

	err = s.storage.GetThing(thingID, thingLoaded)
	require.NoError(s.T(), err)
	assert.EqualValues(s.T(), thing.Features, thingLoaded.Features)
	assert.EqualValues(s.T(), thing.Revision+1, thingLoaded.Revision)

	err = s.storage.GetThingData(thingID, thingLoaded)
	require.NoError(s.T(), err)

	assert.EqualValues(s.T(), thing.Revision+1, thingLoaded.Revision)
	assert.Nil(s.T(), thingLoaded.Features)
}

func (s *PersistenceTestSuite) TestAddThingNoID() {
	_, err := s.storage.AddThing(nil)
	require.Error(s.T(), err)

	_, err = s.storage.AddThing(&model.Thing{})
	require.Error(s.T(), err)
}

func (s *PersistenceTestSuite) TestAddThingInvalidID() {
	thing := &model.Thing{
		ID: &model.NamespacedID{
			Namespace: protocol.TopicPlaceholder,
			Name:      "testName",
		},
	}

	_, err := s.storage.AddThing(thing)
	require.Error(s.T(), err)

	thing.ID.Namespace = "test.namespace"
	thing.ID.Name = protocol.TopicPlaceholder
	_, err = s.storage.AddThing(thing)
	require.Error(s.T(), err)
}

func (s *PersistenceTestSuite) TestDeleteThing() {
	thingID := testThingID + "_TestDeleteThing"

	_, err := s.storage.AddThing(createThing(thingID))
	require.NoError(s.T(), err)
	s.assertSystemThingData(thingID)
	s.assertFeatureSynchState(thingID, testFeatureID1, false)
	s.assertFeatureSynchState(thingID, testFeatureID2, false)

	err = s.storage.RemoveThing(thingID)
	require.NoError(s.T(), err)

	s.assertThingPresent(testThingID, false)

	thingLoaded := &model.Thing{}
	err = s.storage.GetThing(thingID, thingLoaded)
	require.True(s.T(), errors.Is(err, persistence.ErrThingNotFound))

	err = s.storage.GetThingData(thingID, thingLoaded)
	require.True(s.T(), errors.Is(err, persistence.ErrThingNotFound))

	// assert no system data left
	systemData, err := s.storage.GetSystemThingData(thingID)
	require.True(s.T(), errors.Is(err, persistence.ErrThingNotFound))
	require.Nil(s.T(), systemData)
}

func (s *PersistenceTestSuite) TestAddFeature() {
	thing := createThing(testThingID)
	_, err := s.storage.AddThing(thing)
	require.NoError(s.T(), err)

	featuresSize := len(thing.Features)
	revision := thing.Revision

	for featureID, feature := range thing.Features {
		s.addFeature(featureID+"_copy", feature)
		s.assertFeatureSynchState(testThingID, featureID+"_copy", false)
	}
	s.assertFeatureSynchState(testThingID, testFeatureID1, false)

	thingLoaded := &model.Thing{}
	err = s.storage.GetThing(testThingID, thingLoaded)
	require.NoError(s.T(), err)
	assert.EqualValues(s.T(), featuresSize*2, len(thingLoaded.Features))
	assert.EqualValues(s.T(), revision+int64(featuresSize), thingLoaded.Revision)

	// assert feature synchronization state, unsynch revision 1 expected
	s.assertFeatureSynchState(testThingID, testFeatureID1, false)
	ok, _ := s.storage.FeatureSynchronized(testThingID, testFeatureID1, 0)
	assert.False(s.T(), ok, testThingID, testFeatureID1, 0)
	ok, _ = s.storage.FeatureSynchronized(testThingID, testFeatureID1, 1)
	assert.True(s.T(), ok, testThingID, testFeatureID1, 1)
	// second sync call any revision should return ok
	ok, _ = s.storage.FeatureSynchronized(testThingID, testFeatureID1, 0)
	assert.True(s.T(), ok, testThingID, testFeatureID1, 0)

	s.assertFeatureSynchState(testThingID, testFeatureID1, true)
	s.assertFeatureSynchState(testThingID, testFeatureID2, false)

	s.assertStateOnThingSynchronized(thingLoaded.Revision)
}

func (s *PersistenceTestSuite) TestAddFeatureUnsyncTracking() {
	thing := createThing(testThingID)
	_, err := s.storage.AddThing(thing)
	require.NoError(s.T(), err)

	for featureID, feature := range thing.Features {
		// call a second feature change
		s.addFeature(featureID, feature)
		s.assertFeatureSynchState(testThingID, featureID, false)
	}

	// assert feature synchronization state, unsynch revision 2 expected
	s.assertFeatureSynchState(testThingID, testFeatureID1, false)
	ok, _ := s.storage.FeatureSynchronized(testThingID, testFeatureID1, 0)
	assert.False(s.T(), ok, testThingID, testFeatureID1, 0)
	s.assertFeatureSynchState(testThingID, testFeatureID1, false)
	ok, _ = s.storage.FeatureSynchronized(testThingID, testFeatureID1, 2)
	assert.True(s.T(), ok, testThingID, testFeatureID1, 2)

	s.assertFeatureSynchState(testThingID, testFeatureID1, true)
	s.assertFeatureSynchState(testThingID, testFeatureID2, false)
}

func (s *PersistenceTestSuite) assertStateOnThingSynchronized(revision int64) {
	ok, err := s.storage.ThingSynchronized(testThingID, revision)
	assert.NoError(s.T(), err)
	assert.True(s.T(), ok, testThingID, 1)
	s.assertFeatureSynchState(testThingID, testFeatureID1, true)
	s.assertFeatureSynchState(testThingID, testFeatureID2, true)

	systemData := s.assertSystemThingData(testThingID)
	assert.NotNil(s.T(), systemData)
	assert.EqualValues(s.T(), 0, len(systemData.UnsynchronizedFeatures), systemData.UnsynchronizedFeatures)
	assert.EqualValues(s.T(), 0, len(systemData.DeletedFeatures), systemData.UnsynchronizedFeatures)
}

func (s *PersistenceTestSuite) TestDeleteFeature() {
	thingID := testThingID + "_TestRemoveFeature"
	thing := createThing(thingID)
	_, err := s.storage.AddThing(thing)
	require.NoError(s.T(), err)
	defer s.deleteTestThing(thingID)

	featuresSize := len(thing.Features)
	revision := thing.Revision

	for featureID := range thing.Features {
		err = s.storage.RemoveFeature(thingID, featureID)
		require.NoError(s.T(), err)

		s.assertFeatureSynchState(thingID, featureID, true)
		// assert tracked as deleted thing feature
		s.assertFeatureDeletedState(thingID, featureID, true)
	}

	thingLoaded := &model.Thing{}
	err = s.storage.GetThing(thingID, thingLoaded)
	require.NoError(s.T(), err)
	assert.EqualValues(s.T(), 0, len(thingLoaded.Features))
	assert.EqualValues(s.T(), revision+int64(featuresSize), thingLoaded.Revision)

	// assert system data is not cleaned on thing update - with no features
	_, err = s.storage.AddThing(&model.Thing{
		ID: model.NewNamespacedIDFrom(thingID),
	})
	require.NoError(s.T(), err)

	systemThingData := s.assertSystemThingData(thingID)
	assert.EqualValues(s.T(), featuresSize, len(systemThingData.DeletedFeatures), systemThingData.DeletedFeatures)
	assert.EqualValues(s.T(), 0, len(systemThingData.UnsynchronizedFeatures), systemThingData.UnsynchronizedFeatures)
	assert.EqualValues(s.T(), thingLoaded.Revision+1, systemThingData.Revision)

	// assert system data is changed on thing update - with one of the deleted features "re-added"
	delete(thing.Features, testFeatureID1)
	_, err = s.storage.AddThing(thing)
	require.NoError(s.T(), err)

	systemThingData = s.assertSystemThingData(thingID)
	assert.EqualValues(s.T(), featuresSize-1, len(systemThingData.DeletedFeatures), systemThingData.DeletedFeatures)

	_, deleted := systemThingData.DeletedFeatures[testFeatureID1]
	assert.True(s.T(), deleted, systemThingData.DeletedFeatures)

	_, deleted = systemThingData.DeletedFeatures[testFeatureID2]
	assert.False(s.T(), deleted, systemThingData.DeletedFeatures)
	s.assertFeatureSynchState(thingID, testFeatureID1, true)
}

func (s *PersistenceTestSuite) TestGetWithNilInterface() {
	_, err := s.storage.AddThing(createThing(testThingID))
	require.NoError(s.T(), err)
	defer s.deleteThing()

	var thing *model.Thing
	err = s.storage.GetThing(testThingID, thing)
	assert.Error(s.T(), err)

	err = s.storage.GetThingData(testThingID, thing)
	assert.Error(s.T(), err)

	var feature *model.Feature
	err = s.storage.GetFeature(testThingID, testFeatureID1, feature)
	assert.Error(s.T(), err)
}

func (s *PersistenceTestSuite) addThing(thingID string, features map[string]*model.Feature) {
	thing := (&model.Thing{}).
		WithIDFrom(thingID).
		WithFeatures(features)
	_, err := s.storage.AddThing(thing)
	require.NoError(s.T(), err)

	thingLoaded := &model.Thing{}
	err = s.storage.GetThing(thingID, thingLoaded)
	require.NoError(s.T(), err)
	assert.EqualValues(s.T(), thing.ID, thingLoaded.ID)
	assert.EqualValues(s.T(), thing.Features, thingLoaded.Features)

	s.assertSystemThingData(thingID)
}

func (s *PersistenceTestSuite) deleteThing() {
	s.deleteTestThing(testThingID)
}

func (s *PersistenceTestSuite) deleteTestThing(ID string) {
	err := s.storage.RemoveThing(ID)
	require.NoError(s.T(), err)

	s.assertThingPresent(ID, false)

	thingLoaded := &model.Thing{}
	err = s.storage.GetThing(ID, thingLoaded)
	require.Error(s.T(), err)

	// assert that system data is cleared on thing delete
	_, err = s.storage.GetSystemThingData(ID)
	require.Error(s.T(), err)
	require.True(s.T(), errors.Is(err, persistence.ErrThingNotFound))
}

func createThing(ID string) *model.Thing {
	return createThingWithFeatures(ID, testFeatureID1, testFeatureID2)
}

func createThingWithFeatures(ID string, feature1 string, feature2 string) *model.Thing {
	thing := &model.Thing{
		ID:           model.NewNamespacedIDFrom(ID),
		PolicyID:     model.NewNamespacedIDFrom("policy:id"),
		DefinitionID: model.NewDefinitionIDFrom("def:ini:1.0.0"),
		Attributes: map[string]interface{}{
			"key1": 1.22,
			"key2": []interface{}{"a", "b"},
		},
		Features: map[string]*model.Feature{
			feature1: {
				Definition: []*model.DefinitionID{
					model.NewDefinitionIDFrom("def:ini:1.0.0"),
					model.NewDefinitionIDFrom("def:ini.package.data:1.0.0")},
				Properties:        map[string]interface{}{"prop1": "prop1Val", "prop2": 1.234},
				DesiredProperties: map[string]interface{}{"prop1": "prop1ValNew", "prop2": 2.345},
			},
			feature2: {
				Definition: []*model.DefinitionID{
					model.NewDefinitionIDFrom("def:ini2:1.0.0"),
					model.NewDefinitionIDFrom("def:ini2.data:1.0.0")},
				Properties: map[string]interface{}{"prop2": []int{1, 2}},
			},
		},
		Revision:  1,
		Timestamp: "",
	}
	return thing
}

func (s *PersistenceTestSuite) addFeature(featureID string, feature *model.Feature) {
	rev, err := s.storage.AddFeature(testThingID, featureID, feature)
	require.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), rev, int64(1))

	featureLoaded := &model.Feature{}
	err = s.storage.GetFeature(testThingID, featureID, featureLoaded)
	require.NoError(s.T(), err)
	assert.EqualValues(s.T(), feature, featureLoaded)

	s.assertFeatureDeletedState(testThingID, featureID, false)
	s.assertFeatureSynchState(testThingID, featureID, false)
}

func (s *PersistenceTestSuite) assertThingPresent(thingID string, present bool) {
	ids, err := s.storage.GetThingIDs()
	require.NoError(s.T(), err)
	found := false
	for _, id := range ids {
		if thingID == id {
			found = true
			break
		}
	}
	assert.Equal(s.T(), present, found, thingID, ids)
}

func (s *PersistenceTestSuite) assertFeatureDeletedState(thingID string, featureID string, deleted bool) {
	systemData := s.assertSystemThingData(thingID)
	_, present := systemData.DeletedFeatures[featureID]
	assert.Equal(s.T(), deleted, present, systemData.DeletedFeatures)
}

func (s *PersistenceTestSuite) assertFeatureSynchState(thingID string, featureID string, synchronized bool) {
	systemData := s.assertSystemThingData(thingID)
	_, present := systemData.UnsynchronizedFeatures[featureID]
	assert.Equal(s.T(), synchronized, !present, systemData.UnsynchronizedFeatures)
}

func (s *PersistenceTestSuite) assertSystemThingData(thingID string) *data.SystemThingData {
	s.assertThingPresent(thingID, true)

	systemData, err := s.storage.GetSystemThingData(thingID)

	require.NoError(s.T(), err)
	assert.NotNil(s.T(), systemData)
	assert.Equal(s.T(), thingID, systemData.ID)
	return systemData
}

func (s *PersistenceTestSuite) TestUnsynchronizedStateOnUpdates() {
	thingID := testThingID + "_Unsynchronized"
	noIDs := []string{}
	allFeatureIds := []string{testFeatureID1, testFeatureID2,
		testFeatureID1 + "_copy", testFeatureID2 + "_copy"}

	// Add thing with first 2 features
	thing := createThing(thingID)
	_, err := s.storage.AddThing(thing)
	require.NoError(s.T(), err)
	defer s.storage.RemoveThing(thingID)

	s.assertSystemData(thingID, noIDs, allFeatureIds[:2])

	// Remove the first 2 features
	s.removeFeatures(thingID, allFeatureIds[:2])
	s.assertSystemData(thingID, allFeatureIds[:2], noIDs)

	// Add thing with all 2 features
	for featureID, feature := range thing.Features {
		rev, err := s.storage.AddFeature(thingID, featureID, feature)
		require.NoError(s.T(), err)
		assert.Equal(s.T(), int64(1), rev)

		rev, err = s.storage.AddFeature(thingID, featureID+"_copy", feature)
		require.NoError(s.T(), err)
		assert.Equal(s.T(), int64(1), rev)
	}
	s.assertSystemData(thingID, noIDs, allFeatureIds)

	// Remove the first 2 features
	s.removeFeatures(thingID, allFeatureIds[:2])
	systemData := s.assertSystemData(thingID, allFeatureIds[:2], allFeatureIds[2:])

	// Sync one deleted and one modified
	ok, _ := s.storage.FeatureSynchronized(thingID, allFeatureIds[1], 1)
	assert.True(s.T(), ok, allFeatureIds[1], systemData.UnsynchronizedFeatures)

	ok, _ = s.storage.FeatureSynchronized(thingID, allFeatureIds[2], 1)
	assert.True(s.T(), ok, allFeatureIds[1], systemData.UnsynchronizedFeatures)
	s.assertSystemData(thingID, allFeatureIds[:1], allFeatureIds[3:])

	// Add thing again with first 2 features; test all previous marked as deleted
	_, err = s.storage.AddThing(thing)
	require.NoError(s.T(), err)
	s.assertSystemData(thingID, allFeatureIds[2:], allFeatureIds[:2])

	// Remove all features
	s.removeFeatures(thingID, allFeatureIds[:2])
	s.assertSystemData(thingID, allFeatureIds, noIDs)
}

func (s *PersistenceTestSuite) assertSystemData(thingID string,
	deletedFeatures []string, unsynchronizedFeatures []string,
) *data.SystemThingData {
	systemData := s.assertSystemThingData(thingID)
	s.assertDeleted(deletedFeatures, systemData.DeletedFeatures)
	s.assertUnsynchronized(unsynchronizedFeatures, systemData.UnsynchronizedFeatures)
	return systemData
}

func (s *PersistenceTestSuite) assertDeleted(expectedKeys []string, data map[string]interface{}) {
	assert.EqualValues(s.T(), len(expectedKeys), len(data), data)
	for _, key := range expectedKeys {
		_, present := data[key]
		assert.True(s.T(), present)
	}
}

func (s *PersistenceTestSuite) assertUnsynchronized(expectedKeys []string, data map[string]int64) {
	assert.EqualValues(s.T(), len(expectedKeys), len(data), data)
	for _, key := range expectedKeys {
		_, present := data[key]
		assert.True(s.T(), present)
	}
}

func (s *PersistenceTestSuite) removeFeatures(thingID string, featureIDs []string) {
	for _, featureID := range featureIDs {
		err := s.storage.RemoveFeature(thingID, featureID)
		require.NoError(s.T(), err)
	}
}
