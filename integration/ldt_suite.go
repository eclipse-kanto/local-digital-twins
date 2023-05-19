// Copyright (c) 2023 Contributors to the Eclipse Foundation
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

//go:build integration

package integration

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/caarlos0/env/v6"
	"github.com/eclipse-kanto/kanto/integration/util"
	"github.com/eclipse/ditto-clients-golang/model"
	"github.com/eclipse/ditto-clients-golang/protocol"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ldtTestConfiguration struct {
	StatusTimeoutMs             int    `env:"SCT_STATUS_TIMEOUT_MS" envDefault:"10000"`
	StatusReadySinceTimeDeltaMs int    `env:"SCT_STATUS_READY_SINCE_TIME_DELTA_MS" envDefault:"0"`
	StatusRetryIntervalMs       int    `env:"SCT_STATUS_RETRY_INTERVAL_MS" envDefault:"2000"`
	PolicyId                    string `env:"POLICY_ID"`
}

type ldtTestCaseData struct {
	command            *things.Command
	expectedTopic      string
	feature            *model.Feature
	expectedStatusCode int
}

type localDigitalTwinsSuite struct {
	suite.Suite
	util.SuiteInitializer

	thingURL               string
	namespacedID           *model.NamespacedID
	ldtTestConfiguration   *ldtTestConfiguration
	twinEventTopicModified string
	twinEventTopicDeleted  string
	twinEventTopicCreated  string
}

func (suite *localDigitalTwinsSuite) SetupLdtSuite() {
	suite.Setup(suite.T())

	ldtTestCfg := &ldtTestConfiguration{}
	opts := env.Options{RequiredIfNoDef: true}
	require.NoError(suite.T(), env.Parse(ldtTestCfg, opts), "failed to process suite connector test environment variables")
	suite.ldtTestConfiguration = ldtTestCfg
	suite.thingURL = util.GetThingURL(suite.Cfg.DigitalTwinAPIAddress, suite.ThingCfg.DeviceID)
	suite.twinEventTopicModified = util.GetTwinEventTopic(suite.ThingCfg.DeviceID, protocol.ActionModified)
	suite.twinEventTopicCreated = util.GetTwinEventTopic(suite.ThingCfg.DeviceID, protocol.ActionCreated)
	suite.twinEventTopicDeleted = util.GetTwinEventTopic(suite.ThingCfg.DeviceID, protocol.ActionDeleted)
	suite.namespacedID = model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)
	suite.createTestThing((&model.Thing{}).WithID(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).WithPolicyIDFrom(suite.ldtTestConfiguration.PolicyId))
}

func (suite *localDigitalTwinsSuite) TearDownLdtSuite() {
	suite.removeTestThing()
	suite.TearDown()

}

func (suite *localDigitalTwinsSuite) convertToMap(bytes []byte) map[string]interface{} {
	mappedResponse := make(map[string]interface{})
	require.NoError(suite.T(), json.Unmarshal(bytes, &mappedResponse), "could not unmarshal")
	return mappedResponse
}

func (suite *localDigitalTwinsSuite) getFeatureDesiredPropertyValue(featureURL string, property string) ([]byte, error) {
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, fmt.Sprintf(featureDesiredPropertyURLTemplate, featureURL, property), nil)
}

func (suite *localDigitalTwinsSuite) getAllPropertiesOfFeature(featureID string) ([]byte, error) {
	return suite.sendDigitalTwinRequest(featurePropertyURLTemplate, featureID, "")
}

func (suite *localDigitalTwinsSuite) getAllDesiredPropertiesOfFeature(featureID string) ([]byte, error) {
	return suite.sendDigitalTwinRequest(featureDesiredPropertyURLTemplate, featureID, "")
}

func (suite *localDigitalTwinsSuite) getDesiredPropertyOfFeature(featureID string, property string) ([]byte, error) {
	return suite.sendDigitalTwinRequest(featureDesiredPropertyURLTemplate, featureID, property)
}

func (suite *localDigitalTwinsSuite) getPropertyOfFeature(featureID string, property string) ([]byte, error) {
	return suite.sendDigitalTwinRequest(featurePropertyURLTemplate, featureID, property)
}

func (suite *localDigitalTwinsSuite) getFeature(featureID string) ([]byte, error) {
	return suite.sendDigitalTwinRequest("", featureID, "")
}

func (suite *localDigitalTwinsSuite) sendDigitalTwinRequest(urlTemplate, featureID, property string) ([]byte, error) {
	url := ""
	if urlTemplate != "" {
		url = fmt.Sprintf(urlTemplate, util.GetFeatureURL(suite.thingURL, featureID), property)
	} else {
		url = util.GetFeatureURL(suite.thingURL, featureID)
	}

	fmt.Printf("\nURL: %s\n", url)
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, url, nil)
}

func (suite *localDigitalTwinsSuite) getAllFeatures() ([]byte, error) {
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, fmt.Sprintf("%s/features", suite.thingURL), nil)
}

func (suite *localDigitalTwinsSuite) getThing() ([]byte, error) {
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, suite.thingURL, nil)
}

func convertValueToJSON(value interface{}) (map[string]interface{}, error) {
	byteValue, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	jsonValue := make(map[string]interface{})
	if err := json.Unmarshal(byteValue, &jsonValue); err != nil {
		return nil, err
	}

	return jsonValue, nil
}

func (suite *localDigitalTwinsSuite) createTestFeature(feature *model.Feature, featureID string) {
	cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Feature(featureID).Modify(feature)
	require.NoError(suite.T(), suite.DittoClient.Send(cmd.Envelope(protocol.WithResponseRequired(false))), "creation of test feature failed")
}

func (suite *localDigitalTwinsSuite) createTestThing(thing *model.Thing) {
	cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Create(thing)
	require.NoError(suite.T(), suite.DittoClient.Send(cmd.Envelope(protocol.WithResponseRequired(false))), "creation of test thing failed")
}

func (suite *localDigitalTwinsSuite) removeTestFeatures() {
	cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Features().Delete()
	require.NoError(suite.T(), suite.DittoClient.Send(cmd.Envelope(protocol.WithResponseRequired(false))), "removal of test features failed")
}

func (suite *localDigitalTwinsSuite) removeTestThing() {
	cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Delete()
	require.NoError(suite.T(), suite.DittoClient.Send(cmd.Envelope(protocol.WithResponseRequired(false))), "removal of test thing failed")
}

func (suite *localDigitalTwinsSuite) executeCommandEvent(topic string, filter string, newValue interface{}, command *things.Command, expectedPath string, expectedTopic string) {
	ws, err := util.NewDigitalTwinWSConnection(suite.Cfg)
	require.NoError(suite.T(), err, "cannot create a websocket connection to the backend")
	defer ws.Close()

	require.NoError(suite.T(), util.SubscribeForWSMessages(suite.Cfg, ws, util.StartSendEvents, filter), "subscription for events should succeed")
	defer util.UnsubscribeFromWSMessages(suite.Cfg, ws, util.StopSendEvents)

	msg := command.Envelope(protocol.WithResponseRequired(true))

	require.NoError(suite.T(), util.SendMQTTMessage(suite.Cfg, suite.MQTTClient, topic, msg), "unable to send event to the backend")

	result := util.ProcessWSMessages(suite.Cfg, ws, func(msg *protocol.Envelope) (bool, error) {

		if newValue != nil {
			if reflect.TypeOf(newValue).Kind() != reflect.String {
				newValue, err = convertValueToJSON(newValue)
				if err != nil {
					return false, err
				}
			}
		}

		if expectedTopic == msg.Topic.String() && expectedPath == msg.Path && reflect.DeepEqual(msg.Value, newValue) {
			return true, nil
		}

		return false, fmt.Errorf("unexpected value: %s", msg.Value)
	})
	require.NoError(suite.T(), result, "event should be received")
}

func (suite *localDigitalTwinsSuite) executeCommandResponse(command *things.Command) (*protocol.Envelope, error) {
	correlationId, _ := uuid.NewRandom()
	msg := command.Envelope(protocol.WithResponseRequired(true), protocol.WithCorrelationID(correlationId.String()))
	require.NoError(suite.T(), util.SendMQTTMessage(suite.Cfg, suite.MQTTClient, "e", msg), "unable to send event to the backend")
	done := make(chan *protocol.Envelope)

	dittoHandler := func(requestID string, msg *protocol.Envelope) {
		if msg.Headers.CorrelationID() == correlationId.String() && msg.Topic.Action == protocol.ActionRetrieve && msg.Topic.String() == command.Topic.String() {
			done <- msg
		}
		if msg.Headers.CorrelationID() == correlationId.String() && msg.Headers.Originator() != "" && msg.Topic.String() == command.Topic.String() {
			done <- msg
		}
	}

	suite.DittoClient.Subscribe(dittoHandler)
	defer suite.DittoClient.Unsubscribe(dittoHandler)

	select {
	case response := <-done:
		return response, nil
	case <-time.After(30 * time.Second):
		return nil, errors.New("response timeout")
	}
}
