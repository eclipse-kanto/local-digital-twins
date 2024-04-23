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

package persistence

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence/data"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/pkg/errors"
)

// ThingsStorage provides handles things and features model data persistency.
type ThingsStorage interface {
	// GetThingIDs returns the identifiers of the currently stored things.
	// ErrDatabaseClosed is returned on invocation if the database is closed.
	GetThingIDs() ([]string, error)

	// AddThing persists the thing data and its features data.
	// Updates the data if the thing data is already available.
	// Returns the thing's unsynchronized revision value on success.
	AddThing(thing *model.Thing) (int64, error)

	// GetThing retrieves the stored thing data into the pointed thing.
	// Returns ErrorThingNotFound if no thing is found with the provided thing ID.
	GetThing(thingID string, thing *model.Thing) error

	// GetThingData retrieves the stored thing's data into the pointed thing without any features data.
	// To  retrieve the thing with its features also, please use the GetThing function.
	// Returns ErrorThingNotFound if no thing is found with the provided thing ID.
	GetThingData(thingID string, thing *model.Thing) error

	// RemoveThing removes the thing data and all of its features data.
	// Returns ErrorThingNotFound if no thing is found with the provided thing ID.
	RemoveThing(thingID string) error

	// AddFeature persists the feature data. Updates the data if the feature data is already available.
	// Returns ErrorThingNotFound if no thing is found with the provided thing ID.
	// Returns the feature's unsynchronized revision value on success.
	AddFeature(thingID string, featureID string, feature *model.Feature) (int64, error)

	// GetFeature retrieves the stored feature data into the pointed feature.
	// Returns ErrorThingNotFound if no thing is found with the provided thing ID or
	// ErrorFeatureNotFound if the referenced thing has no feature with the provided feature ID.
	GetFeature(thingID string, featureID string, feature *model.Feature) error

	// RemoveFeature removes the persisted feature data.
	// Returns ErrorThingNotFound if no thing is found with the provided thing ID or
	// ErrorFeatureNotFound if the referenced thing has no feature with the provided feature ID.
	RemoveFeature(thingID string, featureID string) error

	// ThingSynchronized removes all thing's system data that is related to thing's synchronization state
	// if the revision matches the current thing's revision. Returns true in such case.
	ThingSynchronized(thingID string, revision int64) (bool, error)

	// FeatureSynchronized removes all data that is related to feature's synchronization state if the revision
	// matches the current feature modification revision or it's marked as deleted.
	// Returns false if the provided revision does not match the system unsynch revision, false otherwise.
	// If the feature is marked as unsynchronized or deleted, its system synchronization data is removed.
	FeatureSynchronized(thingID string, featureID string, revision int64) (bool, error)

	// GetSystemThingData retrieves the system data related to the thing and its features synchronization state.
	GetSystemThingData(thingID string) (*data.SystemThingData, error)

	// GetDeviceID returns the device ID which data is stored into the database.
	GetDeviceID() string

	// Close closes the opened database.
	// ErrDatabaseClosed is returned on invocation of database operation on closed database.
	Close() error
}

var (
	// ErrThingNotFound indicates that a thing with such ID does not exist.
	ErrThingNotFound = errors.Wrap(ErrNotFound, "thing could not be found")

	// ErrFeatureNotFound indicates that a feature with such ID does not exist within the specified thing's features.
	ErrFeatureNotFound = errors.Wrap(ErrNotFound, "feature could not be found")
)

type thingsDB struct {
	deviceID string
	path     string
	db       Database
}

// NewThingsDB opens the things database.
func NewThingsDB(path, deviceID string) (ThingsStorage, error) {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0711); err != nil {
			return nil, errors.Wrapf(err, "error creating directory for device '%s' storage on location '%s'", deviceID, path)
		}
	}

	database, err := NewDatabase(path)
	if err != nil {
		return nil, err
	}

	name, _ := database.GetName()
	if len(name) == 0 {
		database.SetName(deviceID)

	} else {
		if name != deviceID {
			database.Close()
			if err := backupDB(path, name); err != nil {
				return nil,
					errors.Wrapf(err, "error initializing clean device '%s' storage on location '%s'", deviceID, path)
			}
			return NewThingsDB(path, deviceID)
		}
	}

	return &thingsDB{
		deviceID: deviceID,
		path:     path,
		db:       database,
	}, nil
}

func backupDB(path, name string) error {
	backupSuffix := strings.ReplaceAll(name, ":", "_")
	if err := os.Rename(path, fmt.Sprintf("%s.%s", path, backupSuffix)); err != nil {
		return os.Rename(path, fmt.Sprintf("%s.%d", path, time.Now().Unix()))
	}
	return nil
}

func (storage *thingsDB) Close() error {
	return storage.db.Close()
}

func (storage *thingsDB) GetDeviceID() string {
	return storage.deviceID
}

func (storage *thingsDB) GetThingIDs() ([]string, error) {
	things := make(map[string]interface{})
	if err := storage.db.GetAs(data.IDSeparator, &things); err != nil {
		if errors.Is(err, ErrNotFound) {
			return make([]string, 0), nil
		}
		return nil, err
	}
	ids := make([]string, len(things))
	i := 0
	for id := range things {
		ids[i] = id
		i = i + 1
	}
	return ids, nil
}

func (storage *thingsDB) AddThing(thing *model.Thing) (int64, error) {
	if thing == nil || thing.ID == nil {
		return -1, errors.New("thing with provided ID is mandatory on adding thing")
	}

	if thing.ID.Namespace == protocol.TopicPlaceholder ||
		thing.ID.Name == protocol.TopicPlaceholder {
		return -1, errors.Errorf("provided thing ID '%s' is invalid", thing.ID)
	}

	thingID := thing.ID.String()
	thingData, systemThingData, _ := storage.loadThingData(thingID)

	if thingData == nil {
		thingData = &data.ThingData{}
	}

	if systemThingData == nil {
		systemThingData = &data.SystemThingData{
			ID:                     thingID,
			Revision:               thing.Revision - 1,
			DeletedFeatures:        make(map[string]interface{}),
			UnsynchronizedFeatures: make(map[string]int64),
		}
	}

	updateThingData(thingData, thingID, thing)
	updateSystemThingData(systemThingData)
	err := storage.persistThingData(thingData, systemThingData, thing.Features)
	if err == nil {
		storage.updateThingIDs(thingID, true)
	}
	return systemThingData.Revision, err
}

func (storage *thingsDB) GetThing(thingID string, thing *model.Thing) error {
	thingData, systemThingData, err := storage.loadThingData(thingID)

	if err == nil {
		if thing == nil {
			return errors.New("thing interface is expected to fill data in")
		}
		thingData.Value(thing)
		if systemThingData != nil {
			systemThingData.Value(thing)
		}

		var featuresData []interface{}
		featuresData, err = storage.db.GetAllAs(data.FeaturesKeyPrefix(thingID), &data.FeatureData{})
		if err == nil {
			for _, val := range featuresData {
				feature := model.Feature{}
				val.(*data.FeatureData).Value(&feature)
				thing.WithFeature(val.(*data.FeatureData).ID, &feature)
			}
			return nil
		}
	}

	return errors.Wrapf(err, "thing with ID '%s' could not be loaded", thingID)
}

func (storage *thingsDB) GetThingData(thingID string, thing *model.Thing) error {
	thingData, systemThingData, err := storage.loadThingData(thingID)

	if err != nil {
		return errors.Wrapf(err, "thing data for ID '%s' could not be loaded", thingID)
	}
	if thing == nil {
		return errors.New("thing interface is expected to fill data in")
	}

	thingData.Value(thing)
	if systemThingData != nil {
		systemThingData.Value(thing)
	}
	return nil
}

func (storage *thingsDB) GetSystemThingData(thingID string) (*data.SystemThingData, error) {
	return storage.loadSystemThingData(thingID)
}

func (storage *thingsDB) RemoveThing(thingID string) error {
	var err error
	if _, err = storage.loadSystemThingData(thingID); err == nil {
		if err = storage.db.Delete(thingID); err == nil {
			if err = storage.db.DeleteAll(data.FeaturesKeyPrefix(thingID)); err == nil {
				storage.db.DeleteAll(data.SystemThingKey(thingID))
				storage.updateThingIDs(thingID, false)
			}
		}
	}
	return errors.Wrapf(err, "thing data for ID '%s' could not be deleted", thingID)
}

func (storage *thingsDB) AddFeature(thingID string, featureID string, feature *model.Feature) (int64, error) {
	systemThingData, err := storage.updateSystemThingData(thingID)
	if err == nil {
		if revision, err := storage.persistFeature(featureID, feature, systemThingData); err == nil {
			return revision, nil
		}
	}

	return -1, errors.Wrapf(err,
		"feature with ID '%s' on the thing with ID '%s' could not be stored", featureID, thingID)
}

func (storage *thingsDB) GetFeature(thingID string, featureID string, feature *model.Feature) error {
	var err error
	if _, err = storage.loadSystemThingData(thingID); err == nil {
		featureData := data.FeatureData{}
		if err = storage.db.GetAs(data.FeatureKey(thingID, featureID), &featureData); err == nil {
			if feature == nil {
				return errors.New("feature interface is expected to fill data in")
			}
			featureData.Value(feature)
			return nil
		}
		if err == ErrNotFound {
			err = ErrFeatureNotFound
		}
	}
	return errors.Wrapf(err,
		"feature with ID '%s' on the thing with ID '%s' could not be loaded", featureID, thingID)
}

func (storage *thingsDB) RemoveFeature(thingID string, featureID string) error {
	systemThingData, err := storage.updateSystemThingData(thingID)

	if err == nil {
		featureKey := data.FeatureKey(thingID, featureID)
		if _, err = storage.db.Get(featureKey); err == ErrNotFound {
			err = ErrFeatureNotFound
		} else {
			if err = storage.db.Delete(featureKey); err == nil {
				systemThingData.DeletedFeatures[featureID] = nil
				delete(systemThingData.UnsynchronizedFeatures, featureID)
				storage.db.SetAs(systemThingData.Key(), systemThingData)
				return nil
			}
		}
	}
	return errors.Wrapf(err,
		"feature with ID '%s' on the thing with ID '%s' could not be deleted", featureID, thingID)
}

func (storage *thingsDB) ThingSynchronized(thingID string, revision int64) (bool, error) {
	systemThingData, err := storage.loadSystemThingData(thingID)
	if err != nil {
		return false, err
	}

	if revision == systemThingData.Revision {
		systemThingData.DeletedFeatures = make(map[string]interface{})
		systemThingData.UnsynchronizedFeatures = make(map[string]int64)
		if err = storage.db.SetAs(systemThingData.Key(), systemThingData.Data()); err != nil {
			return false, errors.Wrapf(err, "thing system data for ID '%s' could not be updated", thingID)
		}
		return true, nil
	}
	return false, nil
}

func (storage *thingsDB) FeatureSynchronized(thingID string, featureID string, revision int64) (bool, error) {
	systemThingData, err := storage.loadSystemThingData(thingID)
	if err != nil {
		return false, err
	}

	rev, ok := systemThingData.UnsynchronizedFeatures[featureID]
	if !ok {
		if _, deleted := systemThingData.DeletedFeatures[featureID]; !deleted {
			return true, nil
		}
		delete(systemThingData.DeletedFeatures, featureID)

	} else if rev != revision {
		return false, nil

	} else {
		delete(systemThingData.UnsynchronizedFeatures, featureID)
	}

	if err = storage.db.SetAs(systemThingData.Key(), systemThingData.Data()); err != nil {
		return false, errors.Wrapf(err, "thing system data for ID '%s' could not be updated", thingID)
	}
	return true, nil
}

func (storage *thingsDB) loadThingData(thingID string) (*data.ThingData, *data.SystemThingData, error) {
	thingData := data.ThingData{}

	err := storage.db.GetAs(thingID, &thingData)
	if err == nil {
		systemThingData, _ := storage.loadSystemThingData(thingID)
		return &thingData, systemThingData, nil
	}
	if err == ErrNotFound {
		return nil, nil, ErrThingNotFound
	}
	return nil, nil, err
}

func (storage *thingsDB) loadSystemThingData(thingID string) (*data.SystemThingData, error) {
	thingData := data.SystemThingData{}

	err := storage.db.GetAs(data.SystemThingKey(thingID), &thingData)
	if err == nil {
		return &thingData, nil
	}
	if err == ErrNotFound {
		return nil, ErrThingNotFound
	}
	return nil, err
}

func (storage *thingsDB) persistThingData(
	thingData *data.ThingData,
	systemThingData *data.SystemThingData,
	features map[string]*model.Feature,
) error {
	persistData := make(map[string]interface{})

	persistData[thingData.Key()] = thingData.Data()
	persistData[systemThingData.Key()] = systemThingData.Data()

	systemThingData.UnsynchronizedFeatures = make(map[string]int64)
	if prevFeatures, err := storage.db.GetAllAs(data.FeaturesKeyPrefix(thingData.ID), &data.FeatureData{}); err == nil {
		for _, val := range prevFeatures {
			systemThingData.DeletedFeatures[val.(*data.FeatureData).ID] = nil
		}
	}

	for featureID, feature := range features {
		putFeatureData(persistData, featureID, feature, systemThingData)
	}

	return storage.persistAll(thingData.ID, persistData)
}

func putFeatureData(
	persistData map[string]interface{}, featureID string,
	feature *model.Feature, systemThingData *data.SystemThingData,
) {
	featureData := featureData(systemThingData.ID, featureID, feature)
	persistData[featureData.Key()] = featureData.Data()

	delete(systemThingData.DeletedFeatures, featureID)
	systemThingData.UnsynchronizedFeatures[featureID] = systemThingData.UnsynchronizedFeatures[featureID] + 1
}

func (storage *thingsDB) persistAll(thingID string, values map[string]interface{}) error {
	if err := storage.db.UpdateAllAs(data.FeaturesKeyPrefix(thingID), values); err != nil {
		return errors.Wrapf(err, "thing with ID '%s' could not be stored", thingID)
	}
	return nil
}

func updateThingData(data *data.ThingData, thingID string, thing *model.Thing) {
	data.ID = thingID
	data.Attributes = thing.Attributes
	if thing.PolicyID != nil {
		data.PolicyID = thing.PolicyID.String()
	}
	if thing.DefinitionID != nil {
		data.DefinitionID = thing.DefinitionID.String()
	}
}

func (storage *thingsDB) updateSystemThingData(thingID string) (*data.SystemThingData, error) {
	systemThingData, err := storage.loadSystemThingData(thingID)
	if err != nil {
		return nil, err
	}
	updateSystemThingData(systemThingData)
	return systemThingData, nil
}

func updateSystemThingData(systemThingData *data.SystemThingData) {
	systemThingData.Revision = systemThingData.Revision + 1
	systemThingData.Timestamp = time.Now().Format(time.RFC3339)
}

func (storage *thingsDB) updateThingIDs(thingID string, present bool) error {
	things := make(map[string]interface{})
	if err := storage.db.GetAs(data.IDSeparator, &things); err != nil {
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	if present {
		things[thingID] = nil
	} else {
		delete(things, thingID)
	}
	err := storage.db.SetAs(data.IDSeparator, things)
	return err
}

func (storage *thingsDB) persistFeature(featureID string, feature *model.Feature, systemThingData *data.SystemThingData,
) (int64, error) {
	persistData := make(map[string]interface{})

	putFeatureData(persistData, featureID, feature, systemThingData)
	persistData[systemThingData.Key()] = systemThingData.Data()

	return systemThingData.UnsynchronizedFeatures[featureID], storage.db.SetAllAs(persistData)
}

func featureData(thingID string, featureID string, feature *model.Feature) *data.FeatureData {
	fData := &data.FeatureData{
		ID:                featureID,
		ThingID:           thingID,
		Properties:        feature.Properties,
		DesiredProperties: feature.DesiredProperties,
	}
	definitions := feature.Definition
	var dataDefinitions []string
	for _, definition := range definitions {
		dataDefinitions = append(dataDefinitions, definition.String())
	}
	fData.Definition = dataDefinitions
	return fData
}
