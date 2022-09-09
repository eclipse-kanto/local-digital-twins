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

package data

import "github.com/eclipse-kanto/local-digital-twins/internal/model"

// IDSeparator is used for key definition system symbol.
// Should be a control character, i.e. invalid namespace/entityID symbol.
const IDSeparator string = "§"

// StoreData provides key-value persitable pair
type StoreData interface {
	// Key returns the storage key.
	Key() string
	// Data returns the persistable data.
	Data() interface{}
	// Value returns the created instance from the stored data.
	Value(interface{})
}

// ThingData represents the persistable model.Thing structure.
type ThingData struct {
	// ThingID matches the model.Thing namespace ID string representation.
	ID string
	// PolicyID matches the model.Thing policy ID string representation.
	PolicyID string
	// DefinitionID represents the model.Thing definition string representation.
	DefinitionID string
	// Attributes represents the model.Thing attributes.
	Attributes map[string]interface{}
}

// FeatureData represents the persistable model.Feature structure.
type FeatureData struct {
	// ID represents the Feature ID.
	ID string
	// ThingID matches the feature's thing ID, i.e. the ThingData.ID value.
	ThingID string
	// Definition represents model.Feature definitions string representations.
	Definition []string
	// Properties represents model.Feature properties.
	Properties map[string]interface{}
	// Properties represents model.Feature desired properties.
	DesiredProperties map[string]interface{}
}

// SystemThingData is used for Things Storage system data representation.
type SystemThingData struct {
	// ThingID matches the model.Thing namespace ID string representation.
	ID string
	// Revision represents the thing local revision that is initialised with the added model.Thing
	// revision and increased on each thing's data modification, including its features modifications.
	Revision int64
	// Timestamp represents the thing local timestamp that is the timestamp of each
	// thing's data modification, including its features modifications.
	Timestamp string
	// DeletedFeatures is a system field that contains the feature IDs of locally deleted features only,
	// i.e. not synchronized with the remote feature existence state.
	DeletedFeatures map[string]interface{}
	// UnsynchronizedFeatures is a system field that contains the feature IDs of locally modified features only,
	// i.e. not synchronized with the remote feature state.
	// For each unsynchronized feature the revision for its offline change is stored.
	UnsynchronizedFeatures map[string]int64
}

// Key retuens the datatabase key.
func (data *ThingData) Key() string {
	return data.ID
}

// Data returns the persistable data.
func (data *ThingData) Data() interface{} {
	return &data
}

// Value returns the created instance from the stored data.
func (data *ThingData) Value(value interface{}) {
	thing := value.(*model.Thing)
	thing.ID = model.NewNamespacedIDFrom(data.ID)
	if len(data.PolicyID) == 0 {
		thing.PolicyID = nil
	} else {
		thing.PolicyID = model.NewNamespacedIDFrom(data.PolicyID)
	}
	if len(data.DefinitionID) == 0 {
		thing.DefinitionID = nil
	} else {
		thing.DefinitionID = model.NewDefinitionIDFrom(data.DefinitionID)
	}
	thing.Features = nil
	thing.Attributes = data.Attributes
}

// Key returns the datatabase key.
func (data *FeatureData) Key() string {
	return FeatureKey(data.ThingID, data.ID)
}

// Data returns the persistable data.
func (data *FeatureData) Data() interface{} {
	return &data
}

// Value returns the created instance from the stored data.
func (data *FeatureData) Value(value interface{}) {
	value.(*model.Feature).
		WithDefinitionFrom(data.Definition...).
		WithProperties(data.Properties).
		WithDesiredProperties(data.DesiredProperties)
}

// FeatureKey represents a feature database key.
func FeatureKey(thingID string, featureID string) string {
	return thingID + IDSeparator + featureID
}

// FeaturesKeyPrefix represents all thing's features database key prefix.
func FeaturesKeyPrefix(thingID string) string {
	return thingID + IDSeparator
}

// System thing data

// Key returns the datatabase key.
func (data *SystemThingData) Key() string {
	return SystemThingKey(data.ID)
}

// Data returns the persistable data.
func (data *SystemThingData) Data() interface{} {
	return &data
}

// Value returns the created instance from the stored data.
func (data *SystemThingData) Value(value interface{}) {
	thing := value.(*model.Thing)
	thing.Revision = data.Revision
	thing.Timestamp = data.Timestamp
}

// SystemThingKey returns the SystemThingData key.
func SystemThingKey(thingID string) string {
	return IDSeparator + thingID
}
