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
	"fmt"
	"net/http"
	"reflect"

	"github.com/caarlos0/env/v6"
	"github.com/eclipse-kanto/kanto/integration/util"
	"github.com/eclipse/ditto-clients-golang/model"
	"github.com/eclipse/ditto-clients-golang/protocol"
	"github.com/eclipse/ditto-clients-golang/protocol/things"
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
	command       *things.Command
	expectedTopic string
	feature       *model.Feature
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
	require.NoError(suite.T(), env.Parse(ldtTestCfg, opts),
		"failed to process suite connector test environment variables")
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

func (suite *localDigitalTwinsSuite) getFeatureDesiredPropertyValue(featureURL string, property string) ([]byte, error) {
	url := fmt.Sprintf(featureDesiredPropertyURLTemplate, featureURL, property)
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, url, nil)
}

func (suite *localDigitalTwinsSuite) getAllPropertiesOfFeature(featureID string) ([]byte, error) {
	featureURL := util.GetFeatureURL(suite.thingURL, featureID)
	url := fmt.Sprintf(featurePropertyURLTemplate, featureURL, "")
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, url, nil)
}

func (suite *localDigitalTwinsSuite) getAllDesiredPropertiesOfFeature(featureID string) ([]byte, error) {
	featureURL := util.GetFeatureURL(suite.thingURL, featureID)
	url := fmt.Sprintf(featureDesiredPropertyURLTemplate, featureURL, "")
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, url, nil)
}

func (suite *localDigitalTwinsSuite) getDesiredPropertyOfFeature(featureID string, property string) ([]byte, error) {
	featureURL := util.GetFeatureURL(suite.thingURL, featureID)
	url := fmt.Sprintf(featureDesiredPropertyURLTemplate, featureURL, property)
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, url, nil)
}

func (suite *localDigitalTwinsSuite) getPropertyOfFeature(featureID string, property string) ([]byte, error) {
	featureURL := util.GetFeatureURL(suite.thingURL, featureID)
	url := fmt.Sprintf(featurePropertyURLTemplate, featureURL, property)
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, url, nil)
}

func (suite *localDigitalTwinsSuite) getFeature(featureID string) ([]byte, error) {
	featureURL := util.GetFeatureURL(suite.thingURL, featureID)
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, featureURL, nil)
}

func (suite *localDigitalTwinsSuite) getAllFeatures() ([]byte, error) {
	URL := fmt.Sprintf("%s/features", suite.thingURL)
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, URL, nil)
}

func (suite *localDigitalTwinsSuite) getThing() ([]byte, error) {
	return util.SendDigitalTwinRequest(suite.Cfg, http.MethodGet, suite.thingURL, nil)
}

func convertValueToJSON(value interface{}) map[string]interface{} {
	byteValue, _ := json.Marshal(value)
	jsonValue := make(map[string]interface{})
	json.Unmarshal(byteValue, &jsonValue)

	return jsonValue
}

func (suite *localDigitalTwinsSuite) sendDittoCommand(command *things.Command) {
	msg := command.Envelope(protocol.WithResponseRequired(false))
	err := suite.DittoClient.Send(msg)
	require.NoError(suite.T(), err, "command failed")
}

func (suite *localDigitalTwinsSuite) createTestFeature(feature *model.Feature, featureID string) {
	cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Feature(featureID).
		Modify(feature)
	suite.sendDittoCommand(cmd)
}

func (suite *localDigitalTwinsSuite) createTestThing(thing *model.Thing) {
	cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Create(thing)
	suite.sendDittoCommand(cmd)
}

func (suite *localDigitalTwinsSuite) removeTestFeatures() {
	cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Features().Delete()
	suite.sendDittoCommand(cmd)
}

func (suite *localDigitalTwinsSuite) removeTestThing() {
	cmd := things.NewCommand(model.NewNamespacedIDFrom(suite.ThingCfg.DeviceID)).Twin().Delete()
	suite.sendDittoCommand(cmd)
}

func (suite *localDigitalTwinsSuite) executeCommand(topic string, filter string, newValue interface{}, command *things.Command, expectedPath string, expectedTopic string) {
	ws, err := util.NewDigitalTwinWSConnection(suite.Cfg)
	require.NoError(suite.T(), err, "cannot create a websocket connection to the backend")
	defer ws.Close()

	err = util.SubscribeForWSMessages(suite.Cfg, ws, util.StartSendEvents, filter)
	require.NoError(suite.T(), err, "subscription for events should succeed")
	defer util.UnsubscribeFromWSMessages(suite.Cfg, ws, util.StopSendEvents)

	msg := command.Envelope(protocol.WithResponseRequired(true))

	err = util.SendMQTTMessage(suite.Cfg, suite.MQTTClient, topic, msg)
	require.NoError(suite.T(), err, "unable to send event to the backend")

	result := util.ProcessWSMessages(suite.Cfg, ws, func(msg *protocol.Envelope) (bool, error) {

		if newValue != nil {
			if reflect.TypeOf(newValue).Kind() != reflect.String {
				newValue = convertValueToJSON(newValue)
			}
		}

		if expectedTopic == msg.Topic.String() && expectedPath == msg.Path &&
			reflect.DeepEqual(msg.Value, newValue) {
			return true, nil
		}

		return false, fmt.Errorf("unexpected value: %s", msg.Value)
	})
	require.NoError(suite.T(), result, "event should be received")
}
