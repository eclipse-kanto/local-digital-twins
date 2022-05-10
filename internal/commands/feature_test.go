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

package commands_test

import (
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	retrieveFeatureCmd = `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/retrieve",
		%s,
		"path": "/features/meter"
	}`

	featureNotFoundErr = `{
		"topic": "org.eclipse.kanto/test/things/twin/errors",
		%s,
		"path": "/",
		"value": {
			"status": 404,
			"error": "things:feature.notfound",
			"message": "The Feature with ID 'meter' on the Thing with ID 'org.eclipse.kanto:test' could not be found.",
			"description": "Check if the ID of the Thing and the ID of your requested Feature was correct."
		},
		"status": 404
	}`
)

type FeatureCommandsSuite struct {
	CommandsSuite
}

func TestFeatureCommandsSuite(t *testing.T) {
	suite.Run(t, new(FeatureCommandsSuite))
}

func (s *FeatureCommandsSuite) TestFeatureModify() {
	modifyFeatureCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter",
		"value": {
			"properties": {
				"x": 12.34,
				"y": 5.6
			}
		}
	}`

	createdRsp := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter",
		"status": 201
	}`

	createdEvent := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/created",
		%s,
		"path": "/features/meter",
		"value": {
			"properties": {
				"x": 12.34,
				"y": 5.6
			}
		}
	}`

	modifiedEvent := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/modified",
		%s,
		"path": "/features/meter",
		"value": {
			"properties": {
				"x": 12.34,
				"y": 5.6
			}
		}
	}`

	modifiedRsp := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter",
		"status": 204
	}`

	type modifyTest struct {
		input          string
		commandHeaders string
		output         string
		response       string
		event          string
	}

	tests := []modifyTest{
		// successfully created status 201
		{
			commandHeaders: defaultHeaders,
			output:         `{"meter": {"properties": {"x": 12.34, "y": 5.6}}}`,
			response:       createdRsp,
			event:          createdEvent,
		},

		// modify unexisting feature on no response required -> created event, no response
		{
			input:          `{"rectangle": {}}`,
			commandHeaders: headersNoResponseRequired,
			output:         `{"meter": {"properties": {"x": 12.34, "y": 5.6}}}`,
			event:          createdEvent,
		},

		// successfully modified status 204
		{
			input:          `{"meter": {"properties": {"x": 1.2}}}`,
			commandHeaders: defaultHeaders,
			output:         `{"meter": {"properties": {"x": 12.34, "y": 5.6}}}`,
			response:       modifiedRsp,
			event:          modifiedEvent,
		},

		// successfully modified on no response required
		{
			input:          `{"meter": {"properties": {"x": 1.3}}}`,
			commandHeaders: headersNoResponseRequired,
			output:         `{"meter": {"properties": {"x": 12.34, "y": 5.6}}}`,
			event:          modifiedEvent,
		},
	}

	thingOut := model.Thing{}
	for _, test := range tests {

		s.addThing(featuresAsMapValue(s.T(), test.input))
		s.handleCommandF(modifyFeatureCmd, test.commandHeaders)

		s.getThing(&thingOut)
		featuresOutput := featuresAsMapValue(s.T(), test.output)
		assert.EqualValues(s.T(), featuresOutput[testFeatureID], thingOut.Features[testFeatureID])

		assertPublishedOnOkF(s.S(), test.response, test.event)
	}
}

func (s *FeatureCommandsSuite) TestFeatureModifyNonExistingThing() {
	modifyFeatureCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/modify",
		%s,
		"path": "/features/meter",
		"value": {
			"properties": {
				"x": 1.2
			}
		}
	}`

	type modifyTest struct {
		commandHeaders string
		response       string
	}

	tests := []modifyTest{
		// error things:thing.notfound, status 404
		{
			commandHeaders: defaultHeaders,
			response:       thingNotFoundErr,
		},

		// no response required
		{
			commandHeaders: headersNoResponseRequired,
		},
	}

	for _, test := range tests {
		s.handleCommandF(modifyFeatureCmd, test.commandHeaders)
		assertPublishedOnErrorF(s.S(), test.response)
	}
}

func (s *FeatureCommandsSuite) TestRetrieveFeatureThingNotFoundError() {
	s.handleRetrieveCheckResponseF(retrieveFeatureCmd, thingNotFoundErr)
}

func (s *FeatureCommandsSuite) TestFeatureDelete() {
	deleteFeatureCmd := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter"
	}`

	deleteFeatureRsp := `{
		"topic": "org.eclipse.kanto/test/things/twin/commands/delete",
		%s,
		"path": "/features/meter",
		"status": 204
	}`

	deletedEvent := `{
		"topic": "org.eclipse.kanto/test/things/twin/events/deleted",
		%s,
		"path": "/features/meter"
	}`

	type deleteTest struct {
		commandHeaders string
		response       string
	}

	tests := []deleteTest{
		// successfully deleted status 204
		{
			commandHeaders: defaultHeaders,
			response:       deleteFeatureRsp,
		},

		// successfully deleted, no response
		{
			commandHeaders: headersNoResponseRequired,
		},
	}

	for _, test := range tests {
		s.addTestThing()
		s.addFeature(testFeatureID, &model.Feature{})
		s.handleCommandF(deleteFeatureCmd, test.commandHeaders)

		assertPublishedOnDeletedF(s.S(), test.response, deletedEvent)
	}

	err := s.handler.Storage.GetFeature(testThingID, testFeatureID, &model.Feature{})
	require.Error(s.T(), err)

	s.handleRetrieveCheckResponseF(retrieveFeatureCmd, featureNotFoundErr)
}
