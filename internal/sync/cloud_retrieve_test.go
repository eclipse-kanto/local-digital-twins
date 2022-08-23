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
	"fmt"
	"net/http"
	"os"
	"strings"
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
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol/things"
	"github.com/eclipse-kanto/local-digital-twins/internal/sync"
	"github.com/eclipse-kanto/suite-connector/logger"

	"github.com/eclipse-kanto/suite-connector/testutil"
)

const testThingID = "cloud.retrieve:thing"

var (
	namespaceID = model.NewNamespacedIDFrom(testThingID)
	topic       *protocol.Topic
	responseEnv protocol.Envelope
)

type CloudRetrieveSuite struct {
	suite.Suite
	sync *sync.Synchronizer
}

func TestCloudRetrieveSuite(t *testing.T) {
	suite.Run(t, new(CloudRetrieveSuite))
}

func (s *CloudRetrieveSuite) SetupSuite() {
	sync, err := NewTestCloudRetrieveSynchronizer(s.T())
	require.NoError(s.T(), err)
	s.sync = sync
	addTestThing(s)

	topic = &protocol.Topic{
		Namespace: "cloud.retrieve",
		EntityID:  "thing",
		Group:     protocol.GroupThings,
		Channel:   protocol.ChannelTwin,
		Criterion: protocol.CriterionCommands,
		Action:    protocol.ActionRetrieve,
	}
	responseEnv = protocol.Envelope{}
	val := make(map[string]map[string]model.Feature)
	responseEnv.WithTopic(topic).
		WithHeaders(protocol.NewHeaders().
			WithCorrelationID("test/local-digital-twins/sync")).
		WithPath(things.PathThing).
		WithValue(val).
		WithStatus(http.StatusOK)
}

func NewTestCloudRetrieveSynchronizer(t *testing.T) (*sync.Synchronizer, error) {
	db, err := persistence.NewThingsDB(dbLocation, testThingID)
	if err != nil {
		return nil, err
	}

	return &sync.Synchronizer{
		MosquittoPub: &testMosquittoPublisher{
			buffer: list.New(),
		},
		DeviceInfo: commands.DeviceInfo{
			TenantID: "cloud:retrieve:tenant",
		},
		Storage: db,
		Logger:  testutil.NewLogger("sync", logger.TRACE, t),
	}, nil
}

type testMosquittoPublisher struct {
	buffer *list.List
}

func (p *testMosquittoPublisher) Publish(topic string, msgs ...*message.Message) error {
	for _, msg := range msgs {
		msg.Metadata.Set(testAttribute, topic)
		if p.buffer != nil {
			p.buffer.PushBack(msg)
		}
	}
	return nil
}

func (p *testMosquittoPublisher) Close() error {
	return nil
}

func (p *testMosquittoPublisher) Pull() (*message.Message, error) {
	if next := p.buffer.Front(); next != nil {
		return p.buffer.Remove(next).(*message.Message), nil
	}
	return nil, errors.New("no message published")
}

func addTestThing(s *CloudRetrieveSuite) {
	thing := &model.Thing{
		ID: namespaceID,
		Features: map[string]*model.Feature{
			"heater": {
				Properties: map[string]interface{}{
					"water-temperature": 26,
					"air-temperature":   14,
				},
				DesiredProperties: map[string]interface{}{
					"water-temperature": 27,
					"air-temperature":   15,
				},
			},
			"melter": {
				Properties: map[string]interface{}{
					"gold-temperature":     1023,
					"silver-temperature":   961,
					"platinum-temperature": 1767,
				},
				DesiredProperties: map[string]interface{}{
					"gold-temperature":     1024,
					"silver-temperature":   962,
					"platinum-temperature": 1768,
				},
			},
			"depthGauge": {
				Properties: map[string]interface{}{
					"depth": 10,
				},
			},
			"flashlight": {
				Properties: map[string]interface{}{
					"state": "OFF",
				},
				DesiredProperties: map[string]interface{}{
					"state": "ON",
				},
			},
		},
	}
	_, err := s.sync.Storage.AddThing(thing)
	require.NoError(s.T(), err)
}

func (s *CloudRetrieveSuite) TearDownSuite() {
	if s.sync.Storage != nil {
		err := s.sync.Storage.Close()
		require.NoError(s.T(), err)
	}
	s.sync.MosquittoPub.(*testMosquittoPublisher).buffer.Init()

	if err := os.Remove(dbLocation); err != nil {
		s.Error(err, fmt.Sprintf("Error on db test file removal %s", dbLocation))
	}
}

func (s *CloudRetrieveSuite) TestRetrieveDesiredPropertiesCommand() {
	features := map[string]*model.Feature{
		"f1": {
			Properties:        map[string]interface{}{"f1p1": 1, "f1p2": 2, "f1p3": 3},
			DesiredProperties: map[string]interface{}{"f1p1": 11, "f1p2": 12, "f1p3": 13},
		},
		"f2": {
			Properties:        map[string]interface{}{"f2p1": 4, "f2p2": 5, "f2p3": 6},
			DesiredProperties: map[string]interface{}{"f2p1": 44, "f2p2": 55, "f2p3": 66},
		},
		"f3": {
			Properties:        map[string]interface{}{"f3p1": 7, "f3p2": 8, "f3p3": 9},
			DesiredProperties: map[string]interface{}{"f3p1": 77, "f3p2": 88, "f3p3": 99},
		},
	}
	thing := (&model.Thing{}).
		WithID(namespaceID).
		WithFeatures(features)

	replyTo := "command/" + s.sync.DeviceInfo.TenantID
	env := s.sync.RetrieveDesiredPropertiesCommand(thing)
	assertEnv(s.T(), replyTo, env, "f1", "f2", "f3")
}

func assertEnv(t *testing.T, expectedReplyTo string, env *protocol.Envelope, expectedFeaturesIds ...string) {
	assert.Equal(t, protocol.ActionRetrieve, env.Topic.Action)
	assert.Equal(t, expectedReplyTo, env.Headers.ReplyTo())

	fieldsPrefix := "features("
	fieldsSuffix := ")"
	assert.True(t, strings.HasPrefix(env.Fields, fieldsPrefix))
	assert.True(t, strings.HasSuffix(env.Fields, fieldsSuffix))

	fields := strings.TrimPrefix(env.Fields, fieldsPrefix)
	fields = strings.TrimSuffix(fields, fieldsSuffix)
	parts := strings.Split(fields, ",")
	assert.Equal(t, len(expectedFeaturesIds), len(parts))

	for _, featureID := range expectedFeaturesIds {
		assert.Contains(t, parts, featureID+"/desiredProperties")
	}
}

func (s *CloudRetrieveSuite) TestRetrieveDesiredPropertiesCommandNoFeatures() {
	assert.Nil(s.T(), s.sync.RetrieveDesiredPropertiesCommand(&model.Thing{}))
}

func (s *CloudRetrieveSuite) TestHandleResponseInvalidPayload() {
	initialThing := &model.Thing{}
	require.NoError(s.T(), s.sync.Storage.GetThing(testThingID, initialThing))

	cloudResponse := message.NewMessage("invalid", []byte("{[;"))
	msgs, err := s.sync.HandleResponse(cloudResponse)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), msgs)
	assertNoThingUpdate(s, initialThing)
}

func (s *CloudRetrieveSuite) TestHandleResponseUnknownCorrelationID() {
	initialThing := &model.Thing{}
	require.NoError(s.T(), s.sync.Storage.GetThing(testThingID, initialThing))

	payload := `{
		"topic": "cloud.retrieve/thing/things/twin/commands/retrieve",
		"headers": {
			"correlation-id": "unknown"
		},
		"path": "/",
		"value": {},
		"status": 200
	}`
	cloudResponse := message.NewMessage("unknown_id", []byte(payload))
	msgs, err := s.sync.HandleResponse(cloudResponse)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), msgs)
	assertNoThingUpdate(s, initialThing)
}

func assertNoThingUpdate(s *CloudRetrieveSuite, initialThing *model.Thing) {
	thing := &model.Thing{}
	require.NoError(s.T(), s.sync.Storage.GetThing(testThingID, thing))
	assert.Equal(s.T(), initialThing, thing)
}

func (s *CloudRetrieveSuite) TestUpdateLocalDesiredProperties() {
	thing := &model.Thing{}
	require.NoError(s.T(), s.sync.Storage.GetThing(testThingID, thing))

	features := thing.Features

	// no local desired, but "depth" desired property in cloud
	depthGauge := features["depthGauge"]
	assert.EqualValues(s.T(), 10, depthGauge.Properties["depth"])
	assert.Nil(s.T(), depthGauge.DesiredProperties)

	heater := features["heater"]
	assert.EqualValues(s.T(), 14, heater.Properties["air-temperature"])
	assert.EqualValues(s.T(), 26, heater.Properties["water-temperature"])
	assert.Nil(s.T(), heater.Properties["room-temperature"])

	// cloud contains air-temperature,water-temperature and room-temperature
	assert.EqualValues(s.T(), 15, heater.DesiredProperties["air-temperature"])
	assert.EqualValues(s.T(), 27, heater.DesiredProperties["water-temperature"])
	assert.Nil(s.T(), heater.DesiredProperties["room-temperature"])

	melter := features["melter"]
	assert.EqualValues(s.T(), 1023, melter.Properties["gold-temperature"])
	assert.EqualValues(s.T(), 1767, melter.Properties["platinum-temperature"])
	assert.EqualValues(s.T(), 961, melter.Properties["silver-temperature"])
	assert.Nil(s.T(), heater.Properties["aluminum-temperature"])

	// cloud contains only aluminum-temperature desired property
	assert.EqualValues(s.T(), 1024, melter.DesiredProperties["gold-temperature"])
	assert.EqualValues(s.T(), 1768, melter.DesiredProperties["platinum-temperature"])
	assert.EqualValues(s.T(), 962, melter.DesiredProperties["silver-temperature"])
	assert.Nil(s.T(), heater.DesiredProperties["aluminum-temperature"])

	// no cloud desired properties
	flashlight := features["flashlight"]
	assert.EqualValues(s.T(), "OFF", flashlight.Properties["state"])
	assert.EqualValues(s.T(), "ON", flashlight.DesiredProperties["state"])

	assert.Nil(s.T(), features["non-existent-feature"])

	cloudFeatures := map[string]model.Feature{
		"depthGauge": {
			DesiredProperties: map[string]interface{}{
				"depth": 100,
			},
		},
		"heater": {
			DesiredProperties: map[string]interface{}{
				"air-temperature":   22,
				"room-temperature":  21,
				"water-temperature": 28,
			},
		},
		"melter": {
			DesiredProperties: map[string]interface{}{
				"aluminum-temperature": 660,
			},
		},
		"non-existent-feature": {
			DesiredProperties: map[string]interface{}{
				"non-existent-property": false,
			},
		},
	}
	require.NoError(s.T(), s.sync.UpdateLocalDesiredProperties(testThingID, cloudFeatures))

	require.NoError(s.T(), s.sync.Storage.GetThing(testThingID, thing))
	features = thing.Features

	depthGauge = features["depthGauge"]
	assert.EqualValues(s.T(), 10, depthGauge.Properties["depth"])
	assert.EqualValues(s.T(), 100, depthGauge.DesiredProperties["depth"])

	heater = features["heater"]
	assert.EqualValues(s.T(), 14, heater.Properties["air-temperature"])
	assert.EqualValues(s.T(), 26, heater.Properties["water-temperature"])
	assert.Nil(s.T(), heater.Properties["room-temperature"])

	assert.EqualValues(s.T(), 22, heater.DesiredProperties["air-temperature"])
	assert.EqualValues(s.T(), 28, heater.DesiredProperties["water-temperature"])
	assert.EqualValues(s.T(), 21, heater.DesiredProperties["room-temperature"])

	melter = features["melter"]
	assert.EqualValues(s.T(), 1023, melter.Properties["gold-temperature"])
	assert.EqualValues(s.T(), 1767, melter.Properties["platinum-temperature"])
	assert.EqualValues(s.T(), 961, melter.Properties["silver-temperature"])
	assert.Nil(s.T(), heater.Properties["aluminum-temperature"])

	assert.Nil(s.T(), melter.DesiredProperties["gold-temperature"])
	assert.Nil(s.T(), melter.DesiredProperties["platinum-temperature"])
	assert.Nil(s.T(), melter.DesiredProperties["silver-temperature"])
	assert.EqualValues(s.T(), 660, melter.DesiredProperties["aluminum-temperature"])

	flashlight = features["flashlight"]
	assert.EqualValues(s.T(), "OFF", flashlight.Properties["state"])
	assert.Nil(s.T(), flashlight.DesiredProperties["state"])

	assert.Nil(s.T(), features["non-existent-feature"])
}

func (s *CloudRetrieveSuite) TestUpdateLocalDesiredPropertyWithSameValue() {
	featureID := "flashlight"
	feature := &model.Feature{}
	feature.WithDesiredProperty("state", "ON")
	_, err := s.sync.Storage.AddFeature(testThingID, featureID, feature)
	require.NoError(s.T(), err)

	thing := &model.Thing{}
	require.NoError(s.T(), s.sync.Storage.GetThing(testThingID, thing))
	feature = thing.Features[featureID]

	value := map[string]model.Feature{
		"flashlight": {
			DesiredProperties: map[string]interface{}{
				"state": "ON",
			},
		},
	}
	require.NoError(s.T(), s.sync.UpdateLocalDesiredProperties(testThingID, value))

	featureAfterUpdate := &model.Feature{}
	require.NoError(s.T(), s.sync.Storage.GetFeature(testThingID, featureID, featureAfterUpdate))
	assert.Equal(s.T(), feature, featureAfterUpdate) // assert no change
}

func (s *CloudRetrieveSuite) TestUpdateLocalDesiredPropertiesEmptyPayloadValue() {
	require.NoError(s.T(), s.sync.UpdateLocalDesiredProperties(testThingID, map[string]model.Feature{}))

	thingAfterUpdate := &model.Thing{}
	require.NoError(s.T(), s.sync.Storage.GetThing(testThingID, thingAfterUpdate))
	for _, feature := range thingAfterUpdate.Features {
		assert.Nil(s.T(), feature.DesiredProperties)
	}
}

func (s *CloudRetrieveSuite) TestUpdateLocalDesiredPropertiesNonExistentThing() {
	assert.Error(s.T(), s.sync.UpdateLocalDesiredProperties("unknown", map[string]model.Feature{}))
}

func (s *CloudRetrieveSuite) TestValidateResponseValidValues() {
	features := []map[string]model.Feature{
		{},
		{"f1": model.Feature{DesiredProperties: map[string]interface{}{"p1": 1}}},
		nil,
	}

	defaultValue := responseEnv.Value
	value := make(map[string]map[string]model.Feature)
	for _, f := range features {
		if f == nil {
			responseEnv.WithValue(make(map[string]interface{}))
		} else {
			value["features"] = f
			responseEnv.WithValue(value)
		}
		desiredProperties, err := s.sync.RetrievedProperties(responseEnv)
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), desiredProperties)
	}
	responseEnv.WithValue(defaultValue)
}

func (s *CloudRetrieveSuite) TestValidateResponseInvalidPayloadValue() {
	defaultVal := responseEnv.Value
	responseEnv.WithValue(1)
	desiredProperties, err := s.sync.RetrievedProperties(responseEnv)
	assert.Error(s.T(), err)
	assert.Nil(s.T(), desiredProperties)
	responseEnv.WithValue(defaultVal)
}

func (s *CloudRetrieveSuite) TestValidateResponseInvalidTopic() {
	topic.WithGroup(protocol.GroupPolicies)
	responseEnv.WithTopic(topic)
	assertResponseInvalid(s, responseEnv)
	topic.WithGroup(protocol.GroupThings)
	responseEnv.WithTopic(topic)

	topic.WithChannel(protocol.ChannelLive)
	responseEnv.WithTopic(topic)
	assertResponseInvalid(s, responseEnv)
	topic.WithChannel(protocol.ChannelTwin)
	responseEnv.WithTopic(topic)

	topic.WithCriterion(protocol.CriterionEvents)
	responseEnv.WithTopic(topic)
	assertResponseInvalid(s, responseEnv)
	topic.WithCriterion(protocol.CriterionCommands)
	responseEnv.WithTopic(topic)

	topic.WithCriterion(protocol.CriterionErrors)
	responseEnv.WithTopic(topic)
	assertResponseInvalid(s, responseEnv)
	originalVal := responseEnv.Value
	responseEnv.WithValue([5]int{10, 20, 30, 40, 50})
	assertResponseInvalid(s, responseEnv)
	topic.WithCriterion(protocol.CriterionCommands)
	responseEnv.WithTopic(topic).WithValue(originalVal)

	topic.WithAction(protocol.ActionDelete)
	responseEnv.WithTopic(topic)
	assertResponseInvalid(s, responseEnv)
	topic.WithAction(protocol.ActionRetrieve)
	responseEnv.WithTopic(topic)
}

func (s *CloudRetrieveSuite) TestValidateResponseInvalidPath() {
	invalidPaths := []string{
		things.PathThingDefinition,
		things.PathThingAttributes,
		things.PathThingFeatures,
	}

	for _, path := range invalidPaths {
		responseEnv.WithPath(path)
		assertResponseInvalid(s, responseEnv)
	}

	responseEnv.WithPath(things.PathThing)
}

func (s *CloudRetrieveSuite) TestValidateResponseInvalidStatus() {
	invalidStatusCodes := []int{
		http.StatusBadRequest,
		http.StatusBadGateway,
	}

	for _, status := range invalidStatusCodes {
		responseEnv.WithStatus(status)
		assertResponseInvalid(s, responseEnv)
	}

	responseEnv.WithStatus(http.StatusOK)
}

func assertResponseInvalid(s *CloudRetrieveSuite, response protocol.Envelope) {
	desiredProperties, err := s.sync.RetrievedProperties(response)
	assert.NoError(s.T(), err)
	assert.Nil(s.T(), desiredProperties)
}
