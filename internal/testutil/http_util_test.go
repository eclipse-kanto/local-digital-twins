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

//go:build integration_hub
// +build integration_hub

package testutil_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type HttpSuite struct {
	suite.Suite
	test     testutil.HTTPTest
	deviceID string
}

func TestHttpSuite(t *testing.T) {
	suite.Run(t, new(HttpSuite))
}

func (s *HttpSuite) SetupSuite() {
	test, err := testutil.NewHTTPTest()
	require.NoError(s.T(), err)
	s.test = test

	s.deviceID = model.NewNamespacedID(test.Subscription.Namespace, test.Subscription.DeviceName).String()
}

func (s *HttpSuite) TearDownTest() {
	goleak.VerifyNone(s.T())
}

func (s *HttpSuite) TestGetConnectionStatus() {
	req := s.test.NewThingsRequest(s.deviceID).
		Feature("ConnectionStatus").
		Get()
	connectionStatus, err := req.Execute()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), connectionStatus)

	feature := model.Feature{}
	require.NoError(s.T(), json.Unmarshal(connectionStatus, &feature))

	readyUntil := feature.Properties["status"].(map[string]interface{})["readyUntil"]
	timestamp, err := time.Parse(time.RFC3339, readyUntil.(string))
	require.NoError(s.T(), err)
	// When the device is ready to communicate the 'readyUntil' value is '9999-12-31T23:59:59Z'.
	assert.EqualValues(s.T(), 9999, timestamp.Year())
}

func (s *HttpSuite) TestFeatureOperations() {
	featureID := "NewCloudFeature"
	propertyName := "someProperty"

	feature := model.Feature{
		Properties: map[string]interface{}{
			propertyName: 1,
		},
		DesiredProperties: map[string]interface{}{
			propertyName: 2,
		},
	}

	req := s.test.NewThingsRequest(s.deviceID).
		Feature(featureID).
		Put(feature)
	_, err := req.Execute()
	require.NoError(s.T(), err)

	feature.WithDesiredProperties(map[string]interface{}{
		propertyName: 3,
	})
	req.Put(feature)
	_, err = req.Execute()
	require.NoError(s.T(), err)

	req.Get()
	resp, err := req.Execute()
	require.NoError(s.T(), err)
	require.NoError(s.T(), json.Unmarshal(resp, &feature))
	assert.EqualValues(s.T(), 3, feature.DesiredProperties[propertyName])

	req.Delete()
	_, err = req.Execute()
	require.NoError(s.T(), err)
}
