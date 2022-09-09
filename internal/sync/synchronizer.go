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

package sync

import (
	"errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/eclipse-kanto/local-digital-twins/internal/commands"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol/things"
	"github.com/eclipse-kanto/suite-connector/logger"
)

// Synchronizer manages ditto protocol commands using the local digital twins storage.
type Synchronizer struct {
	DeviceInfo   commands.DeviceInfo
	HonoPub      message.Publisher
	MosquittoPub message.Publisher
	Storage      persistence.ThingsStorage

	Logger logger.Logger

	cloudResponsesIDs map[string]string
	connected         bool
}

var (
	// ErrNoConnection indicates that there is no hub connection.
	ErrNoConnection = errors.New("no hub connection")
)

// Start is used to trigger a new synchronization process.
// It will start synchronization for each locally persisted thing.
func (s *Synchronizer) Start() error {
	s.cloudResponsesIDs = make(map[string]string)
	s.connected = true

	thingIDs, err := s.Storage.GetThingIDs()
	if err != nil {
		return err
	}

	err = s.retrieveDesiredProperties(thingIDs...)
	if err != nil {
		s.Logger.Debugf("Error on retrieve desired properties request: %v", err)
	}
	return nil
}

// Stop is used to interrupt a started synchronization process, e.g. on hub connection lost.
func (s *Synchronizer) Stop() {
	s.connected = false
	s.cloudResponsesIDs = make(map[string]string)
}

// Connected is used to modify the connection state.
func (s *Synchronizer) Connected(connected bool) {
	s.connected = connected
}

// SyncThings synchronizes all the things with given IDs.
func (s *Synchronizer) SyncThings(thingIDs ...string) error {
	if !s.connected {
		return ErrNoConnection
	}

	for _, thingID := range thingIDs {
		if err := s.syncThing(thingID); err != nil {
			return err
		}
	}

	return nil
}

func (s *Synchronizer) syncThing(thingID string) error {
	if !s.connected {
		return ErrNoConnection
	}

	s.Logger.Infof("Starting thing '%s' synchronization", thingID)
	sysData, err := s.Storage.GetSystemThingData(thingID)
	if err != nil {
		s.Logger.Errorf("Error on getting thing '%s' system data: %v", thingID, err)
		return err
	}
	syncThing := false
	unsyncFeatures := sysData.UnsynchronizedFeatures
	if len(unsyncFeatures) > 0 {
		syncThing = true
		for featureID, revision := range unsyncFeatures {
			if err := s.syncFeatureRevision(thingID, featureID, revision); err != nil {
				return err
			}
		}
	}

	deletedFeatures := sysData.DeletedFeatures
	if len(deletedFeatures) > 0 {
		syncThing = true
		if err := s.syncDeletedFeatures(thingID, sysData.DeletedFeatures); err != nil {
			return err
		}
	}
	if syncThing {
		ok, err := s.Storage.ThingSynchronized(thingID, sysData.Revision)
		if err != nil {
			s.Logger.Errorf("Error on persisting thing '%s' synchronized state: %v", thingID, err)
			return err
		}

		s.Logger.Infof("Thing '%s' synchronization is finished, synchronized '%v'", thingID, ok)
	} else {
		s.Logger.Debugf("Thing '%s' features were already synchronized", thingID)
	}

	return nil
}

// SyncFeature synchronizes a feature of given thing.
func (s *Synchronizer) SyncFeature(thingID string, featureID string) error {
	if !s.connected {
		return ErrNoConnection
	}

	s.Logger.Debug("Start feature synchronization", logFieldsFeature(thingID, featureID))
	feature := model.Feature{}
	if err := s.Storage.GetFeature(thingID, featureID, &feature); err != nil {
		s.Logger.Error("Error on getting feature", err, logFieldsFeature(thingID, featureID))
		return err
	}

	sysData, err := s.Storage.GetSystemThingData(thingID)
	if err != nil {
		s.Logger.Errorf("Error on getting thing '%s' system data: %v", thingID, err)
		return err
	}

	revision, ok := sysData.UnsynchronizedFeatures[featureID]
	if !ok {
		s.Logger.Debug("The feature is already synchronized", logFieldsFeature(thingID, featureID))
		return nil
	}

	return s.syncFeature(thingID, featureID, &feature, revision)
}

func (s *Synchronizer) syncFeatureRevision(thingID, featureID string, revision int64) error {
	feature := model.Feature{}
	if err := s.Storage.GetFeature(thingID, featureID, &feature); err != nil {
		s.Logger.Error("Error on getting feature", err, logFieldsFeature(thingID, featureID))
		return err
	}

	return s.syncFeature(thingID, featureID, &feature, revision)
}

func (s *Synchronizer) syncFeature(thingID string, featureID string, feature *model.Feature, revision int64) error {
	defHeader := protocol.NewHeaders().
		WithResponseRequired(false).
		WithCorrelationID(watermill.NewUUID())

	featureCmd := featureSyncCmd(model.NewNamespacedIDFrom(thingID), featureID, feature)

	if !s.connected {
		return ErrNoConnection
	}

	if err := publishHonoMsg(featureCmd.Envelope(defHeader), s.HonoPub, s.DeviceInfo, thingID, s.Logger); err != nil {
		return err
	}

	if ok, err := s.Storage.FeatureSynchronized(thingID, featureID, revision); err != nil {
		s.Logger.Debug("Error on persisting feature synchronization state", logFeatureError(thingID, featureID, err))
	} else {
		s.Logger.Debug("Feature synchronization is finished", logFeatureSynchronized(thingID, featureID, ok))
	}
	return nil
}

func featureSyncCmd(thingID *model.NamespacedID, featureID string, thingFeature *model.Feature) *things.Command {
	if len(thingFeature.DesiredProperties) == 0 {
		// No desired properties - publish modify feature
		return things.NewCommand(thingID).
			Feature(featureID).
			Modify(thingFeature)
	}

	// there are desired properties - publish modify feature properties only
	return things.NewCommand(thingID).
		FeatureProperties(featureID).
		Modify(thingFeature.Properties)
}

func (s *Synchronizer) syncDeletedFeatures(thingID string, deletedFeaturesPatch map[string]interface{}) error {
	mergeHeader := protocol.NewHeaders().
		WithResponseRequired(false).
		WithContentType(protocol.ContentTypeJSONMerge).
		WithCorrelationID(watermill.NewUUID())

	featureCmd := things.NewCommand(model.NewNamespacedIDFrom(thingID)).
		Features().
		Merge(deletedFeaturesPatch)

	if !s.connected {
		return ErrNoConnection
	}

	if err := publishHonoMsg(featureCmd.Envelope(mergeHeader), s.HonoPub, s.DeviceInfo, thingID, s.Logger); err != nil {
		return err
	}

	for featureID := range deletedFeaturesPatch {
		if ok, err := s.Storage.FeatureSynchronized(thingID, featureID, 0); err != nil {
			s.Logger.Debug(
				"Error on persisting deleted feature synchronization state",
				logFeatureError(thingID, featureID, err),
			)
		} else {
			s.Logger.Debug("Deleted feature synchronization is finished", logFeatureSynchronized(thingID, featureID, ok))
		}
	}
	return nil
}

func (s *Synchronizer) retrieveDesiredProperties(thingIDs ...string) error {
	for _, thingID := range thingIDs {
		thing := model.Thing{}

		s.Logger.Tracef("Starting retrieve desired properties of thing '%s'", thingID)
		if err := s.Storage.GetThing(thingID, &thing); err != nil {
			return err
		}

		env := s.RetrieveDesiredPropertiesCommand(&thing)

		if !s.connected {
			return ErrNoConnection
		}

		if err := publishHonoMsg(env, s.HonoPub, s.DeviceInfo, thingID, s.Logger); err != nil {
			return err
		}
		s.Logger.Tracef("Retrieve desired properties of thing '%s' published", thingID)
	}

	return nil
}
