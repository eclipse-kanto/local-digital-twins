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

package sync_test

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/eclipse-kanto/local-digital-twins/internal/commands"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/local-digital-twins/internal/sync"
	"github.com/eclipse-kanto/suite-connector/logger"
	"github.com/eclipse-kanto/suite-connector/testutil"
)

const (
	keySeparator string = "@"

	dbLocation      = "things_test_synchronizer.db"
	syncTestThingID = "things.synchronizer:test"
	testAttribute   = "topic.testSynchronizerPublisher"

	testFeatureID1 = "TestFeature1"
	testFeatureID2 = "TestFeature2"
)

type SynchronizerSuite struct {
	suite.Suite
	sync *sync.Synchronizer
}

func TestSynchronizerSuite(t *testing.T) {
	suite.Run(t, new(SynchronizerSuite))
}

func (s *SynchronizerSuite) S() *SynchronizerSuite {
	return s
}

func (s *SynchronizerSuite) SetupSuite() {
	synchronizer, err := NewTestSynchronizer(s.T())
	require.NoError(s.T(), err)
	s.sync = synchronizer
}

func (s *SynchronizerSuite) TearDownSuite() {
	if s.sync.Storage != nil {
		s.sync.Storage.Close()
	}
	if err := os.Remove(dbLocation); err != nil {
		fmt.Printf("Error on %s db test file removal %s", dbLocation, err)
	}
}

func (s *SynchronizerSuite) TearDownTest() {
	s.sync.HonoPub.(*testPublisher).buffer = make(map[string]*list.List)
	s.deleteThing()
}

func (s *SynchronizerSuite) deleteThing() {
	err := s.sync.Storage.RemoveThing(syncTestThingID)
	if errors.Is(err, persistence.ErrThingNotFound) {
		return // not added yet
	}
	require.NoError(s.T(), err)

	thingLoaded := &model.Thing{}
	err = s.sync.Storage.GetThing(syncTestThingID, thingLoaded)
	require.Error(s.T(), err)

}

func NewTestSynchronizer(t *testing.T) (*sync.Synchronizer, error) {
	db, err := persistence.NewThingsDB(dbLocation, syncTestThingID)
	if err != nil {
		return nil, err
	}

	sync := &sync.Synchronizer{
		HonoPub: &testPublisher{
			buffer: make(map[string]*list.List),
		},
		DeviceInfo: commands.DeviceInfo{
			DeviceID: "deviceID",
			TenantID: "tenantID",
		},
		Storage: db,
		Logger:  testutil.NewLogger("sync", logger.TRACE, t),
	}
	sync.Connected(true)
	return sync, nil
}

type testPublisher struct {
	buffer map[string]*list.List
}

func (p *testPublisher) Publish(topic string, msgs ...*message.Message) error {
	for _, msg := range msgs {
		pubEnv := protocol.Envelope{}

		if err := json.Unmarshal(msg.Payload, &pubEnv); err != nil {
			return errors.Wrap(err, "[tests] unexpected message")
		} else {
			if p.buffer != nil {
				key := EnvelopeKey(commands.TopicNamespaceID(pubEnv.Topic), pubEnv.Path)
				if _, ok := p.buffer[key]; !ok {
					p.buffer[key] = list.New()
				}
				p.buffer[key].PushBack(pubEnv)
			}
		}
	}
	return nil
}

func EnvelopeKey(thingID string, path string) string {
	return thingID + keySeparator + path
}

func (p *testPublisher) Close() error {
	return nil
}

func (p *testPublisher) Pull(key string) (protocol.Envelope, error) {
	if envList, ok := p.buffer[key]; ok {
		pubEnv := protocol.Envelope{}
		if next := envList.Front(); next != nil {
			pubEnv = envList.Remove(next).(protocol.Envelope)
		}

		if envList.Len() == 0 {
			delete(p.buffer, key)
		}

		return pubEnv, nil
	}
	return protocol.Envelope{}, errors.New("No message published")
}

func (s *SynchronizerSuite) TestSynchronizeThings() {
	thingID := syncTestThingID + "_FirstThing"
	s.unsynchronizeThing(thingID, true, false)
	defer s.sync.Storage.RemoveThing(thingID)

	thingID2 := syncTestThingID + "_SecondThing"
	s.unsynchronizeThing(thingID2, false, true)
	defer s.sync.Storage.RemoveThing(thingID2)

	err := s.sync.SyncThings(thingID, thingID2)
	require.NoError(s.T(), err)

	pub := s.sync.HonoPub.(*testPublisher)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID, testFeatureID1, true)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID, testFeatureID2, false)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID2, testFeatureID1, false)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID2, testFeatureID2, true)
	assert.Equal(s.T(), 0, len(pub.buffer))
}

func (s *SynchronizerSuite) TestSynchronizeUnexistingThings() {
	thingID := syncTestThingID + "_UnexistingThing"
	err := s.sync.SyncThings(thingID, thingID)
	require.Error(s.T(), err)
}

func (s *SynchronizerSuite) TestSynchronizeThing() {
	s.synchronizeThing("_SynchronizeThing", true, false)
}

func (s *SynchronizerSuite) TestSynchronizeThingAllFeatureWithDesiredProperties() {
	s.synchronizeThing("_WithDesiredProperties", true, true)
}

func (s *SynchronizerSuite) TestSynchronizeThingAllFeatureNoDesiredProperties() {
	s.synchronizeThing("_NoDesiredProperties", false, false)
}

func (s *SynchronizerSuite) TestSunchronizeUnexistingThing() {
	thingID := syncTestThingID + "_UnexistingThing"
	err := s.sync.SyncThings(thingID)
	require.Error(s.T(), err)
}

func (s *SynchronizerSuite) TestSynchronizeFeature() {
	thingID := syncTestThingID + "_Feature"
	s.unsynchronizeThing(thingID, false, false)
	defer s.sync.Storage.RemoveThing(thingID)

	err := s.sync.SyncFeature(thingID, testFeatureID1)
	require.NoError(s.T(), err)

	pub := s.sync.HonoPub.(*testPublisher)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID, testFeatureID1, false)
	assert.Equal(s.T(), 0, len(pub.buffer))
}

func (s *SynchronizerSuite) TestSynchronizeUnexistingFeature() {
	thingID := syncTestThingID + "_UnexistingFeature"
	thing := createThingWithFeatures(thingID, testFeatureID1, false, testFeatureID2, false)
	storage := s.sync.Storage
	_, err := storage.AddThing(thing)
	require.NoError(s.T(), err)
	defer storage.RemoveThing(thingID)

	err = s.sync.SyncFeature(thingID, "UnexistingFeature")
	require.Error(s.T(), err)
}

func (s *SynchronizerSuite) TestSynchronizeFeatureUnexistingThing() {
	thingID := syncTestThingID + "_UnexistingThing"

	err := s.sync.SyncFeature(thingID, "UnexistingFeature")
	require.Error(s.T(), err)
}

func (s *SynchronizerSuite) TestSynchronizeAlreadySynchronizedFeature() {
	thingID := syncTestThingID + "_FeatureAlreadySynchronized"
	thing := createThingWithFeatures(thingID, testFeatureID1, true, testFeatureID2, false)
	storage := s.sync.Storage
	_, err := storage.AddThing(thing)
	require.NoError(s.T(), err)
	err = s.sync.SyncFeature(thingID, testFeatureID1)
	require.NoError(s.T(), err)

	err = s.sync.SyncFeature(thingID, testFeatureID1)
	require.NoError(s.T(), err)
}

func (s *SynchronizerSuite) TestSynchronizeDeleteThing() {
	thingID := syncTestThingID + "_CombineDeleteSynchronizeFeature"
	thing := createThingWithFeatures(thingID, testFeatureID1, true, testFeatureID2, false)
	storage := s.sync.Storage
	_, err := storage.AddThing(thing)
	require.NoError(s.T(), err)

	for featureID, feature := range thing.Features {
		_, err := storage.AddFeature(thingID, featureID, feature)
		require.NoError(s.T(), err)
	}

	err = storage.RemoveFeature(thingID, testFeatureID2)
	require.NoError(s.T(), err)

	err = s.sync.SyncThings(thingID)
	require.NoError(s.T(), err)

	pub := s.sync.HonoPub.(*testPublisher)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID, testFeatureID1, true)

	assertPublishеdEnvelopeOnDelete(s.T(), pub, thingID, testFeatureID2)
	assert.Equal(s.T(), 0, len(pub.buffer))

	sysData, err := storage.GetSystemThingData(thingID)
	assert.NoError(s.T(), err)
	assert.Empty(s.T(), sysData.UnsynchronizedFeatures, sysData.UnsynchronizedFeatures)
	assert.Empty(s.T(), sysData.DeletedFeatures, sysData.DeletedFeatures)
}

func (s *SynchronizerSuite) TestDeleteFeature() {
	thingID := syncTestThingID + "_DeleteFeature"
	thing := createThingWithFeatures(thingID, testFeatureID1, false, testFeatureID2, false)
	storage := s.sync.Storage
	_, err := storage.AddThing(thing)
	require.NoError(s.T(), err)
	defer storage.RemoveThing(thingID)

	err = s.sync.SyncThings(thingID)
	require.NoError(s.T(), err)

	pub := s.sync.HonoPub.(*testPublisher)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID, testFeatureID1, false)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID, testFeatureID2, false)
	assert.Equal(s.T(), 0, len(pub.buffer))

	err = storage.RemoveFeature(thingID, testFeatureID1)
	require.NoError(s.T(), err)

	sysData, err := storage.GetSystemThingData(thingID)
	require.NoError(s.T(), err)

	err = s.sync.SyncThings(thingID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, len(sysData.DeletedFeatures), sysData.DeletedFeatures)

	assertPublishеdEnvelopeOnDelete(s.T(), pub, thingID, testFeatureID1)
	assert.Equal(s.T(), 0, len(pub.buffer))

	sysData, err = storage.GetSystemThingData(thingID)
	assert.NoError(s.T(), err)
	assert.Empty(s.T(), sysData.DeletedFeatures, sysData.DeletedFeatures)

}

func (s *SynchronizerSuite) TestSyncThingNoChanges() {
	thingID := syncTestThingID + "_TestSyncThingNoChanges"
	thing := createThingWithFeatures(thingID, testFeatureID1, false, testFeatureID2, false)
	storage := s.sync.Storage
	_, err := storage.AddThing(thing)
	require.NoError(s.T(), err)
	defer storage.RemoveThing(thingID)

	storage.ThingSynchronized(thingID, 1)
	sysData, err := storage.GetSystemThingData(thingID)
	assert.NoError(s.T(), err)
	assert.Empty(s.T(), sysData.UnsynchronizedFeatures, sysData.UnsynchronizedFeatures)
	assert.Empty(s.T(), sysData.DeletedFeatures, sysData.DeletedFeatures)

	err = s.sync.SyncThings(thingID)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 0, len(s.sync.HonoPub.(*testPublisher).buffer))
}

func (s *SynchronizerSuite) synchronizeThing(
	suffix string, hasDesiredProps1, hasDesiredProps2 bool,
) {
	thingID := syncTestThingID + suffix
	s.unsynchronizeThing(thingID, hasDesiredProps1, hasDesiredProps2)
	defer s.sync.Storage.RemoveThing(thingID)

	err := s.sync.SyncThings(thingID)
	require.NoError(s.T(), err)

	pub := s.sync.HonoPub.(*testPublisher)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID, testFeatureID1, hasDesiredProps1)
	assertPublishedEnvelopeOnModify(s.T(), pub, thingID, testFeatureID2, hasDesiredProps2)
	assert.Equal(s.T(), 0, len(pub.buffer))
}

func (s *SynchronizerSuite) unsynchronizeThing(thingID string, hasDesiredProps1, hasDesiredProps2 bool) {
	thing := createThingWithFeatures(thingID, testFeatureID1, hasDesiredProps1, testFeatureID2, hasDesiredProps2)
	storage := s.sync.Storage
	_, err := storage.AddThing(thing)
	require.NoError(s.T(), err)

	for featureID, feature := range thing.Features {
		_, err := storage.AddFeature(thingID, featureID, feature)
		require.NoError(s.T(), err)
	}
}

func createThingWithFeatures(
	ID string, feature1 string, hasDesiredPropertiesFeature1 bool, feature2 string, hasDesiredPropertiesFeature2 bool,
) *model.Thing {
	thing := &model.Thing{
		ID:           model.NewNamespacedIDFrom(ID),
		PolicyID:     model.NewNamespacedIDFrom("policy:id"),
		DefinitionID: model.NewDefinitionIDFrom("def:ini:1.0.0"),
		Attributes: map[string]interface{}{
			"key1": 1.22,
			"key2": []interface{}{"a", "b"},
		},
		Features: map[string]*model.Feature{
			feature1: createFeature(hasDesiredPropertiesFeature1),
			feature2: createFeature(hasDesiredPropertiesFeature2),
		},
		Revision:  1,
		Timestamp: "",
	}
	return thing
}

func createFeature(hasDesiredProperties bool) *model.Feature {
	if hasDesiredProperties {
		return featureWithDesiredProperties()
	}
	return featureNoDesiredProperties()
}

func featureWithDesiredProperties() *model.Feature {
	feature := model.Feature{}
	feature.Definition = []*model.DefinitionID{
		model.NewDefinitionIDFrom("def:ini:1.0.0"),
		model.NewDefinitionIDFrom("def:ini.package.data:1.0.0")}
	feature.Properties = map[string]interface{}{"prop1": "prop1Val", "prop2": 1.234}
	feature.DesiredProperties = map[string]interface{}{"prop1": "prop1ValNew", "prop2": 2.345}
	return &feature
}

func featureNoDesiredProperties() *model.Feature {
	feature := model.Feature{}
	feature.Definition = []*model.DefinitionID{
		model.NewDefinitionIDFrom("def:ini2:1.0.0"),
		model.NewDefinitionIDFrom("def:ini2.data:1.0.0")}
	feature.Properties = map[string]interface{}{"prop2": []int{1, 2}}
	return &feature
}

func assertPublishedEnvelopeOnModify(t *testing.T, pub *testPublisher, expectedThingID string,
	feature string, hasDesiredPropertiesFeature bool,
) {
	expPath := createPath(feature, hasDesiredPropertiesFeature)
	envKey := EnvelopeKey(expectedThingID, expPath)

	pubEnv, err := pub.Pull(envKey)
	require.NoError(t, err)

	var value map[string]interface{}
	err = json.Unmarshal(pubEnv.Value, &value)
	require.NoError(t, err)

	assert.EqualValues(t, expectedThingID, commands.TopicNamespaceID(pubEnv.Topic))
	assert.EqualValues(t, protocol.ActionModify, pubEnv.Topic.Action)
	assert.EqualValues(t, expPath, pubEnv.Path)
}

func assertPublishеdEnvelopeOnDelete(t *testing.T, pub *testPublisher, expectedThingID string, deletedFeatures ...string) {
	expPath := "/features"
	envKey := EnvelopeKey(expectedThingID, expPath)

	pubEnv, err := pub.Pull(envKey)
	require.NoError(t, err)

	assert.EqualValues(t, expectedThingID, commands.TopicNamespaceID(pubEnv.Topic))
	assert.EqualValues(t, protocol.ActionMerge, pubEnv.Topic.Action)
	assert.EqualValues(t, expPath, pubEnv.Path)

	var value map[string]interface{}
	err = json.Unmarshal(pubEnv.Value, &value)
	require.NoError(t, err)

	assert.EqualValues(t, len(value), len(deletedFeatures))
	expValue := make(map[string]interface{})
	for _, featureID := range deletedFeatures {
		expValue[featureID] = nil
	}

	assert.Equal(t, expValue, value)
}

func createPath(featureID string, hasProperties bool) string {
	path := fmt.Sprintf("/features/%s", featureID)
	if hasProperties {
		path += "/properties"
	}
	return path
}
