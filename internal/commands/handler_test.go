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

package commands_test

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/commands"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/suite-connector/logger"
	"github.com/eclipse-kanto/suite-connector/testutil"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	dbLocation    = "things_test.db"
	testThingID   = "org.eclipse.kanto:test"
	testFeatureID = "meter"
	testAttribute = "topic.testPublisher"

	defaultHeaders = `"headers": {
		"correlation-id": "test/local-digital-twins/commands"
	}`

	responseHeaders = `"headers": {
		"content-type": "application/vnd.eclipse.ditto+json",
		"correlation-id": "test/local-digital-twins/commands",
		"response-required": false
	}`

	headersNoResponseRequired = `"headers": {
		"correlation-id": "test/local-digital-twins/commands",
		"response-required": false
	}`

	createThingEvent = `{
		"topic": "org.eclipse.kanto/test/things/twin/events/created",
		%s,
		"path": "/",
		"value": {
			"thingId": "org.eclipse.kanto:test"
		}
	}`
)

type CommandsSuite struct {
	suite.Suite
	handler      *commands.Handler
	provisioning bool
}

type CommonCommandsSuite struct {
	CommandsSuite
}

func TestCommonCommandsSuite(t *testing.T) {
	suite.Run(t, new(CommonCommandsSuite))
}

func (s *CommandsSuite) S() *CommandsSuite {
	return s
}

func (s *CommandsSuite) SetupSuite() {
	handler, err := NewTestHandler(s.provisioning, s.T())
	require.NoError(s.T(), err)
	s.handler = handler
}

func (s *CommandsSuite) TearDownSuite() {
	if s.handler.Storage != nil {
		s.handler.Storage.Close()
	}
	if err := os.Remove(dbLocation); err != nil {
		fmt.Printf("Error on %s db test file removal %s", dbLocation, err)
	}
}

func (s *CommandsSuite) TearDownTest() {
	s.handler.MosquittoPub.(*testPublisher).buffer.Init()
	s.handler.HonoPub.(*testPublisher).buffer.Init()

	s.deleteThing()
}

func NewTestHandler(autoProvisioning bool, t *testing.T) (*commands.Handler, error) {
	handler := &commands.Handler{}
	db, err := persistence.NewThingsDB(dbLocation, testThingID)
	if err != nil {
		return handler, err
	}
	handler.DeviceID = testThingID
	handler.TenantID = testThingID
	handler.AutoProvisioning = autoProvisioning
	handler.Storage = db
	handler.MosquittoPub = &testPublisher{
		buffer: list.New(),
	}
	handler.HonoPub = &testPublisher{
		buffer: list.New(),
	}
	handler.Logger = testutil.NewLogger("commands", logger.TRACE, t)
	return handler, nil
}

type testPublisher struct {
	buffer *list.List
}

func (p *testPublisher) Publish(topic string, msgs ...*message.Message) error {
	for _, msg := range msgs {
		if msg.Metadata != nil {
			msg.Metadata.Set(testAttribute, topic)
		}
		if p.buffer != nil {
			p.buffer.PushBack(msg)
		}
	}
	return nil
}

func (p *testPublisher) Close() error {
	return nil
}

func (p *testPublisher) Pull() (*message.Message, error) {
	if next := p.buffer.Front(); next != nil {
		return p.buffer.Remove(next).(*message.Message), nil
	}
	return nil, errors.New("no message published")
}

// Tests for LoadThing and LoadFeature utilities without provisioning

func (s *CommonCommandsSuite) TestLoadThingExisting() {
	s.addTestThing()

	thing, err := s.handler.LoadThing(testThingID, &protocol.Envelope{})
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), thing)
}

func (s *CommonCommandsSuite) TestLoadThingUnexisting() {
	thing, err := s.handler.LoadThing(testThingID, &protocol.Envelope{})
	assert.True(s.T(), errors.Is(err, persistence.ErrThingNotFound))
	assert.Nil(s.T(), thing)
}

func (s *CommonCommandsSuite) TestLoadFeatureExistingThing() {
	s.addTestThing()

	feature, err := s.handler.LoadFeature(testThingID, testFeatureID, &protocol.Envelope{})
	assert.True(s.T(), errors.Is(err, persistence.ErrFeatureNotFound))
	assert.Nil(s.T(), feature)
}

func (s *CommonCommandsSuite) TestLoadFeatureExistingFeature() {
	s.addTestThing()
	featureIn := model.Feature{}
	properties := map[string]interface{}{"y": 3.4}
	s.addFeature(testFeatureID, featureIn.WithProperties(properties))
	feature, err := s.handler.LoadFeature(testThingID, testFeatureID, &protocol.Envelope{})
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), feature)
}

func (s *CommonCommandsSuite) TestLoadFeatureUnexistingThing() {
	thing, err := s.handler.LoadFeature(testThingID, testFeatureID, &protocol.Envelope{})
	assert.True(s.T(), errors.Is(err, persistence.ErrThingNotFound))
	assert.Nil(s.T(), thing)
}

func (s *CommonCommandsSuite) TestLoadFeatureUnexistingFeature() {
	s.addTestThing()
	feature, err := s.handler.LoadFeature(testThingID, testFeatureID, &protocol.Envelope{})
	assert.True(s.T(), errors.Is(err, persistence.ErrFeatureNotFound))
	assert.Nil(s.T(), feature)
}

// Tests for commands errors handling

func (s *CommonCommandsSuite) TestUnexpectedCommand() {
	s.addTestThing()

	commands := []string{
		// deleting non existing feature
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
			%s,
			"path": "/features/unknown"
		}`,

		// deleting non existing desired properties
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
			%s,
			"path": "/features/meter/desiredProperties"
		}`,

		// modify properties of unknown feature
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
			%s,
			"path": "/features/unknown/properties",
			"value": 3
		}`,

		// delete property from unknown feature
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
			%s,
			"path": "/features/unknown/properties"
		}`,

		// modify unknown feature property
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
			%s,
			"path": "/features/unknown/properties/foo",
			"value": 5
		}`,

		// delete unknown feature property
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
			%s,
			"path": "/features/unknown/properties/foo"
		}`,
	}

	featureIn := model.Feature{}
	properties := map[string]interface{}{"y": 3.4}
	s.addFeature(testFeatureID, featureIn.WithProperties(properties))

	featureOut := model.Feature{}

	for _, cmd := range commands {
		s.handleCommandF(cmd, defaultHeaders)

		s.getFeature(testFeatureID, &featureOut)
		assert.EqualValues(s.T(), featureIn, featureOut)
	}
}

func (s *CommonCommandsSuite) TestTopicUnsupported() {
	s.addTestThing()

	commands := []string{

		// command topic is unsupported for feature
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/unknown",
			"path": "/features"
		}`,

		// command topic is unsupported for feature
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/unknown",
			"path": "/features/meter"
		}`,

		// command topic is unsupported for all feature's properties
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/unknown",
			"path": "/features/meter/properties"
		}`,

		// command topic is unsupported for properties with path of feature
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/unknown",
			"path": "/features/meter/properties/x/x1"
		}`,
	}

	featureIn := model.Feature{}
	properties := map[string]interface{}{"y": 3.4}
	s.addFeature(testFeatureID, featureIn.WithProperties(properties))

	featureOut := model.Feature{}

	for _, cmd := range commands {
		s.handleCommand(cmd)

		s.getFeature(testFeatureID, &featureOut)
		assert.EqualValues(s.T(), featureIn, featureOut)
	}
}

func (s *CommonCommandsSuite) TestPathUnsupported() {
	s.addTestThing()

	commands := []string{
		// unsupported path for definition
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/create",
			"path": "/definition"
		}`,

		// unsupported path for feature definition
		`{
			"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
			"path": "/features/meter/definition"
		}`,
	}

	featureIn := model.Feature{}
	properties := map[string]interface{}{"y": 3.4}
	s.addFeature(testFeatureID, featureIn.WithProperties(properties))

	featureOut := model.Feature{}

	for _, cmd := range commands {
		s.handleCommand(cmd)

		s.getFeature(testFeatureID, &featureOut)
		assert.EqualValues(s.T(), featureIn, featureOut)
	}
}

func (s *CommonCommandsSuite) TestUnsupportedCriterionTopics() {
	commands := []string{
		// unsupported events topic
		`{
			"topic": "org.eclipse.kanto/test/things/twin/events/delete",
			"path": "/features"
		}`,

		// unsupported search topic
		`{
			"topic": "org.eclipse.kanto/test/things/twin/search/delete",
			"path": "/features"
		}`,

		// unsupported messages topic
		`{
			"topic": "org.eclipse.kanto/test/things/twin/messages/delete",
			"path": "/features"
		}`,

		// unsupported errors topic
		`{
			"topic": "org.eclipse.kanto/test/things/twin/errors/delete",
			"path": "/features"
		}`,
	}

	for _, cmd := range commands {
		s.handleCommand(cmd)
	}
}

func (s *CommonCommandsSuite) TestUnexpectedCommandWithError() {
	s.addTestThing()

	type errorTest struct {
		command  string
		response string
	}
	errorTests := []errorTest{
		// invalid topic namespace
		{
			command: `{
				"topic": "org.eclipse.kanto.f§oo:a-b/test/things/twin/commands/delete",
				%s,
				"path": "/features"
			}`,
		},

		// invalid topic entity name
		{
			command: `{
				"topic": "org.eclipse.kanto/t§est/things/twin/commands/delete",
				%s,
				"path": "/features"
			}`,
		},

		// invalid command payload - path expected as sting
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": ["/features/meter/desiredProperties"]
			}`,
		},

		// invalid command path
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
				%s,
				"path": "unknown/meter/desiredProperties"
			}`,
		},

		// invalid value
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter",
				"value": 3
			}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/errors",
				%s,
				"path": "/",
				"value": {
					"status": 400,
					"error": "json.invalid",
					"message": "Failed to parse command value: json: cannot unmarshal number into Go value of type model.Feature.",
					"description": "Check if the JSON was valid and if it was in required format."
				},
				"status": 400
			}`,
		},

		// modify properties value cannot be parsed
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties",
				"value": 3
			}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/errors",
				%s,
				"path": "/",
				"value": {
					"status": 400,
					"error": "json.invalid",
					"message": "Failed to parse command value: json: cannot unmarshal number into Go value of type map[string]interface {}.",
					"description": "Check if the JSON was valid and if it was in required format."
				},
				"status": 400
			}`,
		},

		// modify property without set value
		{
			command: `{
				"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
				%s,
				"path": "/features/meter/properties/unknown"
			}`,
			response: `{
				"topic": "org.eclipse.kanto/test/things/twin/errors",
				%s,
				"path": "/",
				"value": {
					"status": 400,
					"error": "json.invalid",
					"message": "Failed to parse command value: unexpected end of JSON input.",
					"description": "Check if the JSON was valid and if it was in required format."
				},
				"status": 400
			}`,
		},
	}

	featureIn := model.Feature{}
	properties := map[string]interface{}{"y": 3.4}
	s.addFeature(testFeatureID, featureIn.WithProperties(properties))

	featureOut := model.Feature{}

	for _, errTest := range errorTests {
		s.handleCommandCheckErrorF(errTest.command, defaultHeaders)

		s.getFeature(testFeatureID, &featureOut)
		assert.EqualValues(s.T(), featureIn, featureOut)

		if len(errTest.response) != 0 {
			assertPublishedOnErrorF(s.S(), errTest.response)
		}
	}
}

// Test utilities used in all commands testing suites

func (s *CommandsSuite) handleCommand(payload string) []*message.Message {
	command := &message.Message{
		Payload: []byte(payload),
	}
	msgs, err := s.handler.HandleCommand(command)
	require.NoError(s.T(), err)
	return msgs
}

func (s *CommandsSuite) handleCommandF(payloadFormat string, a ...interface{}) []*message.Message {
	payload := fmt.Sprintf(payloadFormat, a...)
	return s.handleCommand(payload)
}

func (s *CommandsSuite) handleCommandCheckErrorF(payloadFormat string, a ...interface{}) {
	payload := fmt.Sprintf(payloadFormat, a...)
	command := &message.Message{
		Payload: []byte(payload),
	}
	_, err := s.handler.HandleCommand(command)
	assert.NotNil(s.T(), err)
}

func (s *CommandsSuite) addTestThing() {
	thing := (&model.Thing{}).
		WithIDFrom(testThingID)
	rev, err := s.handler.Storage.AddThing(thing)
	require.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), rev, int64(0))
}

func (s *CommandsSuite) addFeature(featureID string, feature *model.Feature) {
	_, err := s.handler.Storage.AddFeature(testThingID, featureID, feature)
	require.NoError(s.T(), err)

	featureLoaded := &model.Feature{}
	err = s.handler.Storage.GetFeature(testThingID, featureID, featureLoaded)
	require.NoError(s.T(), err)
	assert.EqualValues(s.T(), feature, featureLoaded)

	synched, err := s.handler.Storage.FeatureSynchronized(testThingID, featureID, 1)
	require.NoError(s.T(), err)
	require.True(s.T(), synched)
}

func (s *CommandsSuite) getFeature(featureID string, featureOut *model.Feature) {
	err := s.handler.Storage.GetFeature(testThingID, testFeatureID, featureOut)
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), featureOut)
}

func (s *CommandsSuite) getThing(thingOut *model.Thing) {
	err := s.handler.Storage.GetThing(testThingID, thingOut)
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), thingOut)
}

func (s *CommandsSuite) createThing(thing *model.Thing) {
	rev, err := s.handler.Storage.AddThing(thing)
	require.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), rev, int64(0))

	thingLoaded := &model.Thing{}
	err = s.handler.Storage.GetThing(thing.ID.String(), thingLoaded)
	require.NoError(s.T(), err)
	assert.EqualValues(s.T(), thing.ID, thingLoaded.ID)
	assert.EqualValues(s.T(), thing.Features, thingLoaded.Features)
}

func (s *CommandsSuite) addThing(features map[string]*model.Feature) {
	thing := (&model.Thing{}).
		WithIDFrom(testThingID).
		WithFeatures(features)
	s.createThing(thing)
}

func (s *CommandsSuite) deleteCreatedThing(thingID string) {
	err := s.handler.Storage.RemoveThing(thingID)
	if errors.Is(err, persistence.ErrThingNotFound) {
		return // not added yet
	}
	require.NoError(s.T(), err)

	thingLoaded := &model.Thing{}
	err = s.handler.Storage.GetThing(thingID, thingLoaded)
	require.Error(s.T(), err)
}

func (s *CommandsSuite) deleteThing() {
	s.deleteCreatedThing(testThingID)
}

func asMapValue(t *testing.T, data string) map[string]interface{} {
	if len(data) == 0 {
		return nil
	}
	var v map[string]interface{}
	err := json.Unmarshal([]byte(data), &v)
	require.NoError(t, err)
	return v
}

func featuresAsMapValue(t *testing.T, data string) map[string]*model.Feature {
	if len(data) == 0 {
		return nil
	}
	var v map[string]*model.Feature
	err := json.Unmarshal([]byte(data), &v)
	require.NoError(t, err)
	return v
}

func (s *CommandsSuite) asEnvelopeWithID(data string, thingID string) *protocol.Envelope {
	if len(data) == 0 {
		return nil
	}
	env := protocol.Envelope{}
	err := json.Unmarshal([]byte(data), &env)
	require.NoError(s.T(), err)
	s.S().loadVersioning(thingID, &env)
	return &env
}

func (s *CommandsSuite) asEnvelope(data string) *protocol.Envelope {
	return s.asEnvelopeWithID(data, testThingID)
}

func (s *CommandsSuite) asEnvelopeNoValueF(data string) *protocol.Envelope {
	if len(data) == 0 {
		return nil
	}
	return s.asEnvelope(withHeadersNoResponseRequired(data))
}

func (s *CommandsSuite) asEnvelopeWithValueF(data string) *protocol.Envelope {
	if len(data) == 0 {
		return nil
	}
	return s.asEnvelope(withResponseHeadersF(data))
}

func (s *CommandsSuite) loadVersioning(thingID string, envelop *protocol.Envelope) {
	thing := model.Thing{}
	err := s.handler.Storage.GetThingData(thingID, &thing)
	require.NoError(s.T(), err)
	envelop.Revision = thing.Revision
	envelop.Timestamp = thing.Timestamp
}

func (s *CommandsSuite) handleRetrieveCheckResponseF(cmd string, expectedFormat string) {
	if len(expectedFormat) != 0 {
		msgs := s.handleCommandF(cmd, defaultHeaders)
		// assert that response is as expected
		assertPublished(s, withResponseHeadersF(expectedFormat))
		// assert that command not forwarded to the hub
		assert.True(s.T(), len(msgs) == 0)
	}
}

func withResponseHeadersF(envFormat string) string {
	if len(envFormat) == 0 {
		return envFormat
	}
	return fmt.Sprintf(envFormat, responseHeaders)
}

func withHeadersNoResponseRequired(envFormat string) string {
	if len(envFormat) == 0 {
		return envFormat
	}
	return fmt.Sprintf(envFormat, headersNoResponseRequired)
}

func withDefaultHeadersF(envFormat string) string {
	return fmt.Sprintf(envFormat, defaultHeaders)
}

func assertFeatureNotModifiedWithCommand(s *CommandsSuite, input string, cmd string, desiredProps bool) {
	featureIn := model.Feature{}
	featureOut := model.Feature{}

	properties := asMapValue(s.T(), input)
	if desiredProps {
		featureIn.WithDesiredProperties(properties)
	} else {
		featureIn.WithProperties(properties)
	}
	s.addFeature(testFeatureID, &featureIn)

	s.handleCommandF(cmd, defaultHeaders)

	s.getFeature(testFeatureID, &featureOut)
	assert.EqualValues(s.T(), featureIn, featureOut)
}

func assertPublishedOnOkF(s *CommandsSuite, response string, event string) {
	assertPublished(s,
		withHeadersNoResponseRequired(response), // response value not expected, only response status
		s.asEnvelopeWithValueF(event))
}

func assertPublishedOnErrorF(s *CommandsSuite, response string) {
	assertPublished(s,
		withResponseHeadersF(response)) // response value expected as error json
}

func assertPublishedOnDeletedF(s *CommandsSuite, response string, event string) {
	assertPublished(s,
		withHeadersNoResponseRequired(response), // response value not expected, only response status
		s.asEnvelopeNoValueF(event))             // event value not expected
}

func assertPublished(s *CommandsSuite, expected ...interface{}) {
	checkIfPublished(s, false, expected...)
}

func assertPublishedSkipVersioning(s *CommandsSuite, expected ...interface{}) {
	checkIfPublished(s, true, expected...)
}

func assertPublishedNone(s *CommandsSuite) {
	checkIfPublished(s, false)
}

func checkIfPublished(s *CommandsSuite, skipVersioning bool, expected ...interface{}) {
	pub := s.handler.MosquittoPub.(*testPublisher)

	for _, next := range expected {
		if strNext, ok := next.(string); ok && len(strNext) != 0 {
			rsp, err := pub.Pull()
			require.NoError(s.T(), err, strNext)
			assert.JSONEq(s.T(), strNext, string(rsp.Payload))
		} else if nextEnv, ok := next.(*protocol.Envelope); ok {
			rsp, err := pub.Pull()
			require.NoError(s.T(), err, nextEnv)
			msg := &message.Message{
				Payload: []byte(rsp.Payload),
			}
			actualEvent := protocol.Envelope{}
			err = json.Unmarshal(msg.Payload, &actualEvent)
			require.NoError(s.T(), err, nextEnv)
			if skipVersioning {
				assertEnvelopeData(s, nextEnv, actualEvent)
			} else {
				assertEnvelope(s, nextEnv, actualEvent)
			}
		}
	}
	assert.Equal(s.T(), 0, pub.buffer.Len())
}

func assertEnvelope(s *CommandsSuite, expected *protocol.Envelope, actual protocol.Envelope) {
	assertEnvelopeData(s, expected, actual)
	assertEnvelopVersioning(s, expected, actual)
}

func assertEnvelopeData(
	s *CommandsSuite, expected *protocol.Envelope, actual protocol.Envelope,
) {
	assertEnvelopeDataResponseRequiredChanged(s, expected, &actual, false)
}

func assertEnvelopeDataResponseRequiredChanged(
	s *CommandsSuite, expected *protocol.Envelope, actual *protocol.Envelope, nonRequired bool,
) {
	assert.EqualValues(s.T(), expected.Topic, actual.Topic)
	if nonRequired {
		// the response required header should be the only one that difference
		assert.EqualValues(s.T(), expected.Headers, actual.Headers.WithResponseRequired(true))
	} else {
		assert.EqualValues(s.T(), expected.Headers, actual.Headers)
	}
	assert.EqualValues(s.T(), expected.Status, actual.Status)
	assert.EqualValues(s.T(), expected.Path, actual.Path)
	if actual.Value != nil {
		assert.JSONEq(s.T(), string(expected.Value), string(actual.Value))
	}
}

func assertEnvelopVersioning(
	s *CommandsSuite, expected *protocol.Envelope, actual protocol.Envelope,
) {
	assert.EqualValues(s.T(), expected.Revision, actual.Revision)
	assert.EqualValues(s.T(), expected.Timestamp, actual.Timestamp)
}

func assertHonoMsgPublished(s *CommandsSuite) *protocol.Envelope {
	pub := s.handler.HonoPub.(*testPublisher)
	rsp, err := pub.Pull()
	require.NoError(s.T(), err)
	msg := &message.Message{
		Payload: []byte(rsp.Payload),
	}
	actualEvent := protocol.Envelope{}
	err = json.Unmarshal(msg.Payload, &actualEvent)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 0, pub.buffer.Len())
	return &actualEvent
}
