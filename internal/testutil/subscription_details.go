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

package testutil

import (
	// Force resource embed
	_ "embed"
	"encoding/json"

	"github.com/eclipse-kanto/local-digital-twins/internal/model"
	"github.com/pkg/errors"
)

// SubscriptionDetails is used in multiple test files.
type SubscriptionDetails struct {
	GrantType         string `json:"grantType,omitempty"`
	ClientID          string `json:"clientId,omitempty"`
	ClientSecret      string `json:"clientSecret,omitempty"`
	ClientScopeThings string `json:"clientScopeThings,omitempty"`
	Namespace         string `json:"namespace,omitempty"`
	DeviceName        string `json:"deviceName,omitempty"`
	TenantID          string `json:"tenantId,omitempty"`
}

//go:embed test_data.json
var subData []byte

// readSubscriptionTestData returns the embedded 'test_data.json' as SubscriptionDetails structure.
func readSubscriptionTestData() (*SubscriptionDetails, error) {
	var data *SubscriptionDetails
	if err := json.Unmarshal(subData, &data); err != nil {
		return nil, errors.New("file 'test_data.json' is not a valid JSON")
	}
	if err := data.validate(); err != nil {
		return nil, errors.Wrapf(err, "'test_data.json' contains invalid test subscription information")
	}
	return data, nil
}

func (s *SubscriptionDetails) validate() error {
	deviceID := model.NewNamespacedID(
		s.Namespace,
		s.DeviceName,
	)
	if deviceID == nil {
		return errors.Errorf("invalid testing device: namespace '%s', name '%s'", s.Namespace, s.DeviceName)
	}

	return nil
}
