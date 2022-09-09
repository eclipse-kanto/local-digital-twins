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

package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	httpScheme                 = "https"
	httpHostThings             = "things.eu-1.bosch-iot-suite.com"
	httpHostAccess             = "access.bosch-iot-suite.com:443"
	httpPathThingsFormat       = "/api/2/things/%s"
	httpPathThingsEntityFormat = httpPathThingsFormat + "/%s"
)

// HTTPTest is a structure used for automatic setup and creation of http related tests.
type HTTPTest struct {
	Subscription *SubscriptionDetails
	authToken    string
	client       *http.Client
}

// NewHTTPTest returns a new HTTPTest.
func NewHTTPTest() (HTTPTest, error) {
	subsDetails, err := readSubscriptionTestData()
	if err != nil {
		return HTTPTest{}, err
	}

	httpTest := HTTPTest{
		Subscription: subsDetails,
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	httpTest.client = &http.Client{
		Transport: transport,
		Timeout:   time.Minute,
	}

	token, err := suiteAuthToken(httpTest.client, *subsDetails)
	if err != nil {
		return HTTPTest{}, err
	}
	httpTest.authToken = token

	return httpTest, nil
}

func suiteAuthToken(client *http.Client, subsDetails SubscriptionDetails) (string, error) {
	reqBody := testClientDetails(subsDetails)
	URL := tokenURL()
	req, err := http.NewRequest("POST", URL.String(), strings.NewReader(reqBody))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	respBodyData, err := send(client, req)
	if err != nil {
		return "", err
	}

	respBody := make(map[string]interface{})
	if err = json.Unmarshal(respBodyData, &respBody); err != nil {
		return "", err
	}

	token, ok := respBody["access_token"]
	if !ok {
		return "", fmt.Errorf("access token not acquired for subscription with details: %+v", subsDetails)
	}
	return token.(string), nil
}

// ThingsHTTPRequest represents an HTTP request used to retrieve or manipulate remote thing data.
type ThingsHTTPRequest struct {
	url          url.URL
	method       string
	data         interface{}
	authToken    string
	deviceID     string
	relativePath string
	client       *http.Client
}

// NewThingsRequest returns a new ThingsHTTPRequest.
func (t *HTTPTest) NewThingsRequest(deviceID string) *ThingsHTTPRequest {
	URL := url.URL{
		Scheme: httpScheme,
		Host:   httpHostThings,
	}
	return &ThingsHTTPRequest{
		url:       URL,
		deviceID:  deviceID,
		authToken: t.authToken,
		client:    t.client,
	}
}

// Get sets the value of the method of r to "GET".
func (r *ThingsHTTPRequest) Get() *ThingsHTTPRequest {
	r.method = "GET"
	return r
}

// Put sets the value of the method of r to "PUT".
func (r *ThingsHTTPRequest) Put(data interface{}) *ThingsHTTPRequest {
	r.method = "PUT"
	r.data = data
	return r
}

// Delete sets the value of the method of r to "DELETE".
func (r *ThingsHTTPRequest) Delete() *ThingsHTTPRequest {
	r.method = "DELETE"
	return r
}

// Feature sets the relative path of r using featureID to refer to the specified feature.
func (r *ThingsHTTPRequest) Feature(featureID string) *ThingsHTTPRequest {
	r.relativePath = fmt.Sprintf("features/%s", featureID)
	return r
}

// Execute creates a new http.Request using the data from r and executes it using a http.Client.
// Returns the body of the received http.Response.
func (r *ThingsHTTPRequest) Execute() ([]byte, error) {
	body, err := json.Marshal(r.data)
	if err != nil {
		return nil, err
	}

	if r.relativePath == "" {
		r.url.Path = fmt.Sprintf(httpPathThingsFormat, r.deviceID)
	} else {
		r.url.Path = fmt.Sprintf(httpPathThingsEntityFormat, r.deviceID, r.relativePath)
	}

	request, err := http.NewRequest(r.method, r.url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	request.Header.Set("Authorization", "Bearer "+r.authToken)
	return send(r.client, request)
}

func send(client *http.Client, req *http.Request) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return respBody, nil
}

func tokenURL() url.URL {
	URL := url.URL{
		Scheme: httpScheme,
		Host:   httpHostAccess,
		Path:   "/token",
	}
	return URL
}

func testClientDetails(subsDetails SubscriptionDetails) string {
	clientDetails := url.Values{}
	clientDetails.Set("grant_type", subsDetails.GrantType)
	clientDetails.Set("client_id", subsDetails.ClientID)
	clientDetails.Set("client_secret", subsDetails.ClientSecret)
	clientDetails.Set("scope", subsDetails.ClientScopeThings)

	return clientDetails.Encode()
}
