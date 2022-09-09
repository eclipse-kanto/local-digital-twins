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
	"encoding/json"
	"net/http"
	"reflect"
	"strings"

	"github.com/eclipse-kanto/local-digital-twins/internal/commands"
	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol"
	"github.com/eclipse-kanto/local-digital-twins/internal/protocol/things"
	"github.com/eclipse-kanto/suite-connector/logger"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// RetrieveDesiredPropertiesCommand returns a command, which can be used to retrieve the provided thing's desired
// properties from the cloud.
func (s *Synchronizer) RetrieveDesiredPropertiesCommand(thing *model.Thing) *protocol.Envelope {
	length := len(thing.Features)
	if length == 0 {
		return nil
	}
	i := 0
	fieldsBuilder := strings.Builder{}
	fieldsBuilder.WriteString("features(")
	for featureID := range thing.Features {
		fieldsBuilder.WriteString(featureID)
		fieldsBuilder.WriteString("/desiredProperties")
		if i < length-1 {
			fieldsBuilder.WriteString(",")
		}
		i++
	}
	fieldsBuilder.WriteString(")")

	return things.NewCommand(thing.ID).
		Retrieve().
		Envelope(protocol.NewHeaders().
			WithCorrelationID(s.newCorrelationID(thing.ID.String())).
			WithReplyTo("command/" + s.DeviceInfo.TenantID)).
		WithFields(fieldsBuilder.String())
}

// HandleResponse checks and manages retrieve desired properties commands' responses.
func (s *Synchronizer) HandleResponse(msg *message.Message) ([]*message.Message, error) {
	env := protocol.Envelope{}
	if err := json.Unmarshal(msg.Payload, &env); err != nil {
		s.Logger.Debugf("Unexpected cloud to device command payload: %v", err)
		return []*message.Message{msg}, nil
	}

	thingID := model.NewNamespacedID(env.Topic.Namespace, env.Topic.EntityID).String()
	correlationID := env.Headers.CorrelationID()

	if expectedThingID, ok := s.cloudResponsesIDs[correlationID]; ok {
		if expectedThingID != thingID {
			s.Logger.Errorf(
				"Correlation-id '%s' and thing '%s' pair mismatch on desired properties response",
				correlationID,
				thingID,
			)
		} else {
			responseValue, err := s.RetrievedProperties(env)
			if responseValue != nil {
				if err = s.UpdateLocalDesiredProperties(thingID, responseValue); err == nil {
					delete(s.cloudResponsesIDs, correlationID)

					s.cloudResponseHandled(thingID)
				}
			}
			return nil, err
		}
	}
	return []*message.Message{msg}, nil
}

func (s *Synchronizer) cloudResponseHandled(thingID string) {
	if err := s.SyncThings(thingID); err != nil {
		s.Logger.Debugf("Error on synchronizing thing %s: %v ", thingID, err)
	}
}

// UpdateLocalDesiredProperties overwrites the locally persisted desired properties with the provided response value.
func (s *Synchronizer) UpdateLocalDesiredProperties(
	thingID string,
	cloudFeatures map[string]model.Feature,
) error {
	localThing := &model.Thing{}
	if err := s.Storage.GetThing(thingID, localThing); err != nil {
		if errors.Is(err, persistence.ErrThingNotFound) {
			return errors.Wrap(err, "error on updating desired properties")
		}
		s.Logger.Debugf("Error on updating desired properties and getting thing '%'s: %v", thingID, err)
	}

	data, err := s.Storage.GetSystemThingData(thingID)
	if err != nil {
		s.Logger.Debugf("Error on updating desired properties and getting thing '%s' system data: %v", thingID, err)
	}

	featureNotSynchronizedBeforeUpdate := true
	for featureID, localFeature := range localThing.Features {
		if err = s.Storage.GetFeature(thingID, featureID, localFeature); err != nil {
			if errors.Is(err, persistence.ErrThingNotFound) {
				return errors.Wrap(err, "error on updating desired properties")
			}
		}

		if !desiredPropertiesChangedOnSync(featureID, cloudFeatures, localFeature) {
			continue
		}

		if data != nil {
			_, featureNotSynchronizedBeforeUpdate = data.UnsynchronizedFeatures[featureID]
		}
		if _, err = s.Storage.AddFeature(thingID, featureID, localFeature); err != nil {
			s.Logger.Debug("Error on updating feature desired properties", logFeatureError(thingID, featureID, err))
			continue
		}

		if err = s.publishDesiredPropertiesModified(thingID, featureID, localFeature); err != nil {
			s.Logger.Debug(
				"Unable to publish local event on updating desired properties with the cloud values",
				logFeatureError(thingID, featureID, err))
		}

		if !featureNotSynchronizedBeforeUpdate {
			if _, err = s.Storage.FeatureSynchronized(thingID, featureID, 1); err != nil {
				s.Logger.Debug("Error on synchronizing feature", logFeatureError(thingID, featureID, err))
			}
		}
	}
	return nil
}

func desiredPropertiesChangedOnSync(
	ID string,
	cloudFeatures map[string]model.Feature,
	localFeature *model.Feature,
) bool {
	if cloudFeature, ok := cloudFeatures[ID]; ok {
		if reflect.DeepEqual(localFeature.DesiredProperties, cloudFeature.DesiredProperties) {
			return false
		}
		localFeature.WithDesiredProperties(cloudFeature.DesiredProperties)
	} else {
		if len(localFeature.DesiredProperties) == 0 {
			return false
		}
		localFeature.WithDesiredProperties(nil)
	}
	return true
}

// RetrievedProperties extracts features' desired properties from an envelope.
// Returns error if the content is with unexpected topic, path, status or value.
func (s *Synchronizer) RetrievedProperties(env protocol.Envelope) (map[string]model.Feature, error) {
	if !responseValid(env, s.Logger) {
		return nil, nil
	}
	responseValue := make(map[string]map[string]model.Feature)
	if err := json.Unmarshal(env.Value, &responseValue); err != nil {
		return nil, err
	}

	data := responseValue["features"]
	if data == nil {
		return make(map[string]model.Feature), nil
	}
	return data, nil
}

func responseValid(env protocol.Envelope, logger logger.Logger) bool {
	topic := env.Topic
	if topic.Criterion == protocol.CriterionErrors {
		thingsErr := commands.ThingError{}
		errMsg := "Retrieve desired properties response error received"
		if err := json.Unmarshal(env.Value, &thingsErr); err != nil {
			logger.Errorf("%s: %s", errMsg, env)
		} else {
			logger.Errorf("%s: %+v", errMsg, thingsErr)
		}
		return false
	}

	if topic.Criterion != protocol.CriterionCommands || topic.Action != protocol.ActionRetrieve ||
		topic.Channel != protocol.ChannelTwin || topic.Group != protocol.GroupThings {
		logger.Errorf(
			"Unexpected topic '%s' on retrieve desired properties response for correlation-id '%s'",
			topic,
			env.Headers.CorrelationID(),
		)
		return false
	}

	if env.Path != things.PathThing {
		logger.Errorf(
			"Unexpected path '%s' on retrieve desired properties response for correlation-id '%s'",
			env.Path,
			env.Headers.CorrelationID(),
		)
		return false
	}

	if env.Status >= http.StatusBadRequest {
		logger.Errorf(
			"Unexpected status '%s' on retrieve desired properties response for correlation-id '%s'",
			env.Status,
			env.Headers.CorrelationID(),
		)
		return false
	}

	return true
}

func (s *Synchronizer) newCorrelationID(thingID string) string {
	if s.cloudResponsesIDs == nil {
		s.cloudResponsesIDs = make(map[string]string)
	}
	correlationID := watermill.NewUUID()
	s.cloudResponsesIDs[correlationID] = thingID
	return correlationID
}

func (s *Synchronizer) publishDesiredPropertiesModified(
	thingID, featureID string, localFeature *model.Feature,
) error {
	thing := &model.Thing{}
	if err := s.Storage.GetThing(thingID, thing); err != nil {
		return err
	}

	event := things.NewEvent(model.NewNamespacedIDFrom(thingID)).
		FeatureDesiredProperties(featureID).
		Modified(localFeature.DesiredProperties)
	env := event.Envelope(protocol.NewHeaders().
		WithResponseRequired(false).
		WithContentType(protocol.ContentTypeDitto))
	env.Timestamp = thing.Timestamp
	env.Revision = thing.Revision

	data, err := json.Marshal(env)
	if err != nil {
		return err
	}

	message := message.NewMessage(watermill.NewUUID(), []byte(data))
	return s.MosquittoPub.Publish(
		commands.EventPublishTopic(s.DeviceInfo.DeviceID, env.Topic), message)
}
