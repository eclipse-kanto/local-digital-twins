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
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ProvisioningCommandsSuite struct {
	CommandsSuite
}

func TestProvisioningCommandsSuite(t *testing.T) {
	base := &CommandsSuite{
		provisioning: true,
	}
	suite.Run(t, &ProvisioningCommandsSuite{
		*base,
	})
}

// Tests for LoadThing and LoadFeature utilities with provisioning

func (s *ProvisioningCommandsSuite) TestLoadThingUnexistingProvisioning() {
	thing, err := s.handler.LoadThing(testThingID, createEnvelope())
	require.NoError(s.T(), err)
	assert.NotNil(s.T(), thing)
	assertPublishedSkipVersioning(s.S(), s.asEnvelopeWithValueF(createThingEvent))
}

func (s *ProvisioningCommandsSuite) TestLoadFeatureUnexistingThingProvisioning() {
	feature, err := s.handler.LoadFeature(testThingID, testFeatureID, createEnvelope())
	assert.True(s.T(), errors.Is(err, persistence.ErrFeatureNotFound))
	assert.Nil(s.T(), feature)
	assertPublishedSkipVersioning(s.S(), s.asEnvelopeWithValueF(createThingEvent))
}

func (s *ProvisioningCommandsSuite) TestProvisioningFeatureModify() {
	modifyFeatureCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter",
		"value": {
			"properties": {
				"x": 3.1,
				"y": 2.7
			}
		}
	}`

	s.handleCommandF(modifyFeatureCmd, defaultHeaders)
	// successfully created status 201
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter",
		"status": 201
	}`
	event := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/created",
		%s,
		"path": "/features/meter",
		"value": {
			"properties": {
				"x": 3.1,
				"y": 2.7
			}
		}
	}`
	assertPublishedSkipVersioning(s.S(),
		s.asEnvelopeWithValueF(createThingEvent),
		withHeadersNoResponseRequired(response),
		s.asEnvelopeWithValueF(event))
}

func (s *ProvisioningCommandsSuite) TestProvisioningFeatureRetrieve() {
	s.assertThingProvisionedOnCmd(retrieveFeatureCmd, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningFeatureDelete() {
	deleteFeatureCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter"
	}`

	s.assertThingProvisionedOnCmd(deleteFeatureCmd, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningFeaturesModify() {
	// modify features when no thing
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features",
		"value": {
			"meter": {
				"properties": {
					"x": 3.1,
					"y": 2.7
				}
			}
		}
	}`

	s.handleCommandF(command, defaultHeaders)
	response := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features",
		"status": 201
	}`
	event := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/created",
		%s,
		"path": "/features",
		"value": {
			"meter": {
				"properties": {
					"x": 3.1,
					"y": 2.7
				}
			}
		}
	}`
	assertPublishedSkipVersioning(s.S(),
		s.asEnvelopeWithValueF(createThingEvent),
		withHeadersNoResponseRequired(response),
		s.asEnvelopeWithValueF(event))
}

func (s *ProvisioningCommandsSuite) TestProvisioningFeaturesRetrieve() {
	s.assertThingProvisionedOnCmd(retrieve, featuresNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningFeaturesDelete() {
	// no thing and try to delete features
	s.assertThingProvisionedOnCmd(deleteFeaturesCmd, featuresNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningPropertyModify() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter/properties/x",
		"value": {
			"x2": 2.0
		}
	}`

	s.assertThingProvisionedOnCmd(command, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningRetrieveProperty() {
	s.assertThingProvisionedOnCmd(retrieveXCmd, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningPropertyDelete() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter/properties/x"
	}`

	s.assertThingProvisionedOnCmd(command, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningDesiredPropertyModify() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter/desiredProperties/x",
		"value": {
			"x2": 2.0
		}
	}`

	s.assertThingProvisionedOnCmd(command, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningDesiredPropertyRetrieve() {
	s.assertThingProvisionedOnCmd(retrieveDesiredXCmd, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningDesiredPropertyDelete() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter/desiredProperties/x"
	}`

	s.assertThingProvisionedOnCmd(command, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningPropertiesModify() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter/properties",
		"value": {
			"x": 1
		}
	}`

	s.assertThingProvisionedOnCmd(command, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningPropertiesRetrieve() {
	s.assertThingProvisionedOnCmd(retrievePropertiesCmd, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningPropertiesDelete() {
	s.assertThingProvisionedOnCmd(deletePropertiesCmd, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningDesiredPropertiesModify() {
	command := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter/desiredProperties",
		"value": {
			"x": 1.0
		}
	}`

	s.assertThingProvisionedOnCmd(command, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningDesiredPropertiesRetrieve() {
	s.assertThingProvisionedOnCmd(retrieveDesiredPropertiesCmd, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) TestProvisioningDesiredPropertiesDelete() {
	s.assertThingProvisionedOnCmd(deleteDesiredPropertiesCmd, featureNotFoundErr)
}

func (s *ProvisioningCommandsSuite) assertThingProvisionedOnCmd(commandFormat string, expErr string) {
	command := withDefaultHeadersF(commandFormat)
	s.handleCommand(command)
	assertPublishedSkipVersioning(s.S(),
		s.asEnvelopeWithValueF(createThingEvent),
		withResponseHeadersF(expErr))
}

func createEnvelope() *protocol.Envelope {
	topic := protocol.Topic{}
	topic.WithNamespace("org.eclipse.kanto").
		WithEntityID("test").
		WithGroup(protocol.GroupThings).
		WithChannel(protocol.ChannelTwin).
		WithCriterion(protocol.CriterionCommands).
		WithAction(protocol.ActionRetrieve)
	headers := protocol.NewHeaders().WithCorrelationID("test/local-digital-twins/commands")
	envelope := protocol.Envelope{}
	return envelope.WithPath("/").WithTopic(&topic).WithHeaders(headers)
}
