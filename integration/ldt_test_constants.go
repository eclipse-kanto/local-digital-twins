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

import "github.com/eclipse/ditto-clients-golang/model"

const (
	featurePropertyURLTemplate        = "%s/properties/%s"
	featureDesiredPropertyURLTemplate = "%s/desiredProperties/%s"
	desiredProperty                   = "testDesiredProperty"
	featureID                         = "testFeature"
	property                          = "testProperty"
	value                             = "testValue"
)

var (
	emptyFeature                 = &model.Feature{}
	featureWithDesiredProperties = (&model.Feature{}).WithDesiredProperty(desiredProperty, value)
	featureWithProperties        = (&model.Feature{}).WithProperty(property, value)
	properties                   = map[string]interface{}{desiredProperty: value}
	features                     = map[string]*model.Feature{featureID: emptyFeature}
)
