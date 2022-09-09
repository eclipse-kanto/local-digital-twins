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

package jsonutil

import (
	"fmt"
	"strings"

	parser "github.com/Jeffail/gabs/v2"
	"github.com/pkg/errors"
)

// JSONSubset generates a new JSON object adding the field selector defined values only.
// For JSON Arrays use the JSONArraySubset function.
// Returns error if the provided JSON string is not valid JSON object.
//
// For field selector parsing to set of JSON pointers is used SelectorToJSONPointers utility.
// For example:
// 1. "attributes/model,attributes/location", would select only model and location
// attribute values (if present)
// 2. "attributes(model,location)" would select only model and location attribute values (if present)
// 3. "features/feature1/properties(city,street) would select from city and street values inside
// properties inside feature1 of features (if present)
// 4. "features(feature1/properties,feature1,feature2/properties/country(city,street),feature2)"
// would select only properties inside feature1 inside feature and city and street inside country
// inside properties inside feature2 inside features. Note that feature1 and feature2 inside features
// will not be selected.
func JSONSubset(input, selector string) (string, error) {
	container, pointers, err := parse(input, selector)
	if err != nil {
		return "", err
	}

	return subsetJSON(container, pointers), nil
}

// JSONArraySubset generates a new JSON array adding the field selector defined values only for each array element.
// Returns error if the provided JSON string is not valid JSON array.
func JSONArraySubset(input, selector string) (string, error) {
	if strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]") {
		container, pointers, err := parse(input, selector)
		if err != nil {
			return "", err
		}

		dataArr := container.Children()
		if dataArr != nil {
			arrOutput := ""
			for _, data := range dataArr {
				jsonWithFields := subsetJSON(data, pointers)

				if len(arrOutput) != 0 {
					arrOutput += ","
				}
				arrOutput += jsonWithFields
			}

			arrOutput = fmt.Sprintf("[%s]", arrOutput)
			return arrOutput, nil
		}
	}

	return "", errors.New("JSON array is expected")
}

func parse(input, selector string) (*parser.Container, []string, error) {
	container, err := parser.ParseJSON([]byte(input))
	if err != nil {
		return nil, nil, err
	}

	pointers, err := SelectorToJSONPointers(selector)
	if err != nil {
		return nil, nil, err
	}

	return container, pointers, nil
}

func subsetJSON(input *parser.Container, pointers []string) string {
	subset := parser.New()
	for _, pointer := range pointers {
		jsonValue, _ := input.JSONPointer(pointer)
		if jsonValue != nil {
			if pathSlice, err := parser.JSONPointerToSlice(pointer); err == nil {
				subset.Set(jsonValue, pathSlice...)
			}
		}
	}
	return subset.String()
}
