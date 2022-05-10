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

package jsonutil

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

const (
	openingParenthesis = '('
	closingParenthesis = ')'

	fieldSeparator = ','
	pathSeparator  = "/"

	pathFormatter = "%s" + pathSeparator + "%s"
)

// The JSON fields selector contains a comma-separated list of fields to be included in the returned JSON.
// It defines selecting sub-fields of objects by wrapping sub-fields inside parentheses, as
// a comma-separated list of sub-fields (a sub-field is a JSON pointer separated with /), or
// sub-selectors can be used to request only specific sub-fields by placing expressions
// in parentheses after a selected sub-field.

// SelectorToJSONPointers returns array of the JSON pointers (https://tools.ietf.org/html/rfc6901)
// for a given fields selector string.
//
// Rule example:
// 1. thingId,attributes/model,attributes/location and
//    thingId,attributes(model,location), would return the same array:
// [
//	 /thingId,
//	 /attributes/model,
//	 /attributes/location,
// ]
// 2. features/feature1/properties(city,street), would return array:
// [
//	 /features/feature1/properties/city,
//	 /features/feature1/properties/street,
// ]
// 3. features(feature1/properties,feature1,feature2/properties/country(city,street),feature2),
// would return array, including only most inner paths:
// [
//	 /features/feature1/properties,
//	 /features/feature2/properties/country/city,
//	 /features/feature2/properties/country/street,
// ], i.e. features/feature1 and features/feature2 are not returned.
func SelectorToJSONPointers(selector string) ([]string, error) {
	if err := validateJSONFieldSelector(selector); err != nil {
		return nil, err
	}

	paths := flattenToJSONPointers(innerSelectors(selector))

	for i, next := range paths {
		paths[i] = nodePath("", next)
	}
	return paths, nil
}

func validateJSONFieldSelector(field string) error {
	if strings.Count(field, string(openingParenthesis)) != strings.Count(field, string(closingParenthesis)) {
		return errors.Errorf(
			"the field selector '%s' is with different amount of opening '(' and closing ')' parentheses",
			field)
	}

	return nil
}

// innerSelectors returns array of first level inner selectors.
//
// Processes the submitted selector and divides it into first level if any,
// using the pattern: selector := inner[,inner]*.
//
// Opening "(" and closing ")" parentheses are treated as inner "level" of the selector
// so any commas inside are skipped.
//
// For example:
// 1. attributes(model,location) would return array:
// [
//	 attributes(model,location),
// ]
// 2. thingId,features/someFeature/properties(city/postCode,street) would return array:
// [
//	 thingId,
//	 features/someFeature/properties(city/postCode,street),
// ]
func innerSelectors(parent string) []string {
	if err := validateJSONFieldSelector(parent); err != nil {
		return make([]string, 0)
	}

	children := make([]string, 0)
	var sb strings.Builder

	closingParenthesisCnt := 0
	for _, c := range parent {
		if c == openingParenthesis {
			closingParenthesisCnt++
			sb.WriteRune(c)
			continue
		}
		if closingParenthesisCnt > 0 {
			if c == closingParenthesis {
				closingParenthesisCnt--
			}
			sb.WriteRune(c)
			continue
		}

		if c == fieldSeparator {
			children = addUnique(children, sb.String())
			sb.Reset()
		} else {
			sb.WriteRune(c)
		}
	}
	children = addUnique(children, sb.String())

	return children
}

func flattenToJSONPointers(rawSelectors []string) []string {
	pointersMap := make(map[string]interface{})

	for _, selector := range rawSelectors {
		if !strings.ContainsRune(selector, openingParenthesis) {
			pointersMap[selector] = nil

		} else {
			p := flattenCommonSelectorToJSONPointers(selector)
			for _, pointer := range p {
				pointersMap[pointer] = nil
			}
		}
	}

	ensureMostInnerFields(pointersMap)

	pointers := make([]string, 0)
	for key := range pointersMap {
		if len(key) > 0 {
			pointers = append(pointers, key)
		}
	}
	return pointers
}

// ensureMostInnerFields removes field selectors "a" and "a/b" if "a/b/c" is present also.
func ensureMostInnerFields(fields map[string]interface{}) {
	for field := range fields {
		pathNodes := strings.Split(field, pathSeparator)
		length := len(pathNodes) - 1

		var parent string
		for i, node := range pathNodes {
			if i != length && len(node) > 0 {
				if i == 0 {
					parent = node
				} else {
					parent = nodePath(parent, node)
				}
				delete(fields, parent)
			}
		}
	}
}

// flattenCommonSelectorToJSONPointers for given "a/b(c,d/e)" fields selector returns array with
// "a/b/c" and "a/b/d/e" as elements.
func flattenCommonSelectorToJSONPointers(stringWithParentheses string) []string {
	subStr := strings.SplitN(stringWithParentheses, string(openingParenthesis), 2)
	if len(subStr[1]) == 0 {
		return make([]string, 0)
	}

	strWithoutParentheses := subStr[1][:len(subStr[1])-1]
	innerPaths := flattenToJSONPointers(innerSelectors(strWithoutParentheses))

	root := subStr[0]
	hasRoot := len(root) > 0

	pointers := make([]string, 0)
	if len(innerPaths) == 0 {
		if hasRoot {
			pointers = append(pointers, root)
		}
	} else {
		for _, path := range innerPaths {
			if len(path) > 0 {
				if hasRoot {
					pointers = append(pointers, nodePath(root, path))
				} else {
					pointers = append(pointers, path)
				}
			}
		}
	}
	return pointers
}

func nodePath(root, inner string) string {
	return fmt.Sprintf(pathFormatter, root, inner)
}

func addUnique(slice []string, elem string) []string {
	for _, next := range slice {
		if next == elem {
			return slice
		}
	}
	return append(slice, elem)
}
