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

package jsonutil_test

import (
	"testing"

	"github.com/eclipse-kanto/local-digital-twins/internal/jsonutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fieldsTest struct {
	input  string
	output []string
}

func TestSelectorToJSONPointer(t *testing.T) {
	tests := []fieldsTest{
		{
			input:  "",
			output: []string{},
		},
		{
			input:  ".",
			output: []string{"/."},
		},
		{
			input: "thingId",
			output: []string{
				"/thingId",
			},
		},
		{
			input: "thingId,features",
			output: []string{
				"/thingId",
				"/features",
			},
		},
		{
			input: "thingId,features(square,circle/color)",
			output: []string{
				"/thingId",
				"/features/circle/color",
				"/features/square",
			},
		},
		{
			input: "features(circle(color),square)",
			output: []string{
				"/features/circle/color",
				"/features/square",
			},
		},
		{
			input: "features()",
			output: []string{
				"/features",
			},
		},
		{
			input: "(thingId,features,_revision)",
			output: []string{
				"/thingId",
				"/features",
				"/_revision",
			},
		},
		{
			input: "(thingId,features(,),_revision)",
			output: []string{
				"/thingId",
				"/features",
				"/_revision",
			},
		},
		{
			input: "thingId,features(triangle(),triangle/properties(test(configuration)),square)",
			output: []string{
				"/thingId",
				"/features/triangle/properties/test/configuration",
				"/features/square",
			},
		},
		{
			input: "thingId. /\\%$#@!*&^(f_-+=),features(triangle,triangle/properties,square)",
			output: []string{
				"/thingId. /\\%$#@!*&^/f_-+=",
				"/features/triangle/properties",
				"/features/square",
			},
		},
		// dot or colon in feature name
		{
			input: "thingId,attributes,features(geo:triangle,geo.triangle/properties(type/bySides,color),circle)",
			output: []string{
				"/thingId",
				"/attributes",
				"/features/geo:triangle",
				"/features/geo.triangle/properties/type/bySides",
				"/features/geo.triangle/properties/color",
				"/features/circle",
			},
		},
	}

	for _, test := range tests {
		assertFunc(t, test)
	}
}

func TestSelectorToJSONPointerDuplicatePathElement(t *testing.T) {
	tests := []fieldsTest{
		{
			input: "root(t,t/p/type/byAngles,square,t/p/type/bySides,t,circle),root/t/p/type(byAngles,p,test)",
			output: []string{
				"/root/t/p/type/byAngles",
				"/root/t/p/type/bySides",
				"/root/t/p/type/test",
				"/root/t/p/type/p",
				"/root/circle",
				"/root/square",
			},
		},
		{
			input: "root(t,t/test/test/byAngles,square,t/test/test/bySides,t,circle),root/t/test/test(byAngles,p,test)",
			output: []string{
				"/root/t/test/test/byAngles",
				"/root/t/test/test/bySides",
				"/root/t/test/test/test",
				"/root/t/test/test/p",
				"/root/circle",
				"/root/square",
			},
		},
		{
			input: "test(test,test(test/test)),test/test,test(test/test(test))",
			output: []string{
				"/test/test/test/test",
			},
		},
	}

	for _, test := range tests {
		assertFunc(t, test)
	}
}

func TestSelectorToJSONPointerInconsistentParentheses(t *testing.T) {
	tests := []fieldsTest{
		{
			input: "thingId,features)square,circle/color(",
			output: []string{
				"/thingId",
				"/features)square",
			},
		},
		{
			input: "thingId,features)triangle)(,triangle/properties)test)configuration((,square(",
			output: []string{
				"/thingId",
				"/features)triangle)/triangle/properties)test)configuration",
			},
		},
	}

	for _, test := range tests {
		assertFunc(t, test)
	}
}

func TestSelectorToJSONPointErensureMostInnerFields(t *testing.T) {
	output := []string{
		"/thingId",
		"/f/circle/p/color",
		"/f/square/p/size",
		"/f/trg/p/color",
		"/f/trg/p/size",
		"/attr/Info",
		"/attr/foo",
		"/attr/bar/baz",
		"/_rev",
	}
	inputs := []string{
		// only inner paths: omit "f(circle)" "f(square/p)" "f(trg(p))"
		"thingId,f(circle/p/color,square/p/size,trg/p(color,size),circle,square/p,trg(p)),attr(Info,foo,bar/baz),_rev",

		// only inner paths: omit "f(circle)" "f(trg)"
		"thingId,f(circle,square/p/size,trg/p(color,size)),attr(Info,foo,bar/baz),_rev,f(circle/p/color,trg)",
	}

	for _, input := range inputs {
		assertPointers(t, input, output)
	}
}

func TestSelectorToJSONPointerComplexFilter(t *testing.T) {
	output := []string{
		"/thingId",
		"/f/circle/p/color",
		"/f/square/p/size",
		"/f/triangle/p/color",
		"/f/triangle/p/size",
		"/attr/Info",
		"/attr/foo",
		"/attr/bar/baz",
		"/_rev",
	}
	inputs := []string{
		// long input combined in f(circle...,square...,triangle...)
		"thingId,f(circle/p/color,square/p/size,triangle/p(color,size)),attr(Info,foo,bar/baz),_rev",
		"thingId,f(circle(p/color),square/p/size,triangle/p(color,size)),attr(Info,foo,bar/baz),_rev",
		"thingId,f(circle(p(color)),square/p/size,triangle/p(color,size)),attr(Info,foo,bar/baz),_rev",
		"thingId,f(circle(p(color)),square/p(size),triangle/p(color,size)),attr(Info,foo,bar/baz),_rev",
		"thingId,f(circle(p(color)),square(p(size)),triangle/p(color,size)),attr(Info,foo,bar/baz),_rev",
		"thingId,f(circle(p(color)),square(p(size)),triangle(p(color,size))),attr(Info,foo,bar/baz),_rev",
		"thingId,f(circle(p(color)),square(p(size)),triangle(p/color,p/size)),attr(Info,foo,bar/baz),_rev",
		"thingId,f/circle(p/color),f(square/p/size,triangle/p(color,size)),attr(Info,foo,bar/baz),_rev",

		// separated: f/triangle... and f(square...,circle...)
		"thingId,f/triangle/p(color,size),f(square/p/size,circle/p/color),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square/p/size,circle/p(color)),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square/p/size,circle(p(color))),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square/p(size),circle(p(color))),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square(p/size),circle(p(color))),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square(p(size)),circle(p(color))),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square(p(size)),circle(p/color)),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square(p(size)),circle/p/color),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square/p(size),circle/p/color),attr(Info,foo,bar(baz)),_rev",

		// separated
		"thingId,f/circle/p/color,f/triangle/p(color,size),f(square/p/size),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f/circle/p/color,f(square/p/size),attr(Info,foo,bar(baz)),_rev",
		"thingId,f/triangle/p(color,size),f(square/p/size),attr(Info,foo,bar(baz)),f/circle(p/color),_rev",
		"thingId,f/triangle/p(color,size),f(square/p/size),attr(Info,foo,bar/baz),_rev,f/circle/p(color)",
	}

	for _, input := range inputs {
		assertPointers(t, input, output)
	}
}

func TestSelectorToJSONPointerMoreComplexFilter(t *testing.T) {
	output := []string{
		"/f/circle/p/color",
		"/f/sq/p/size",
		"/f/trg/p/color",
		"/f/trg/p/size",
		"/f/trg/p/type/byAngles",
		"/f/trg/p/type/bySides",
		"/a/Info",
		"/a/foo",
		"/a/bar/baz",
		"/_rev",
	}
	inputs := []string{
		"f(circle/p/color,sq/p/size,trg/p(color,size,type(byAngles,bySides))),a(Info,foo,bar/baz),_rev",
		"f(circle/p/color,sq/p/size,trg/p(color,size,type/byAngles,type/bySides)),a(Info,foo,bar/baz),_rev",
		"f(circle/p/color,sq/p/size,trg/p(color,size)),a(Info,foo,bar/baz),f/trg/p(type/byAngles,type/bySides),_rev",
		"f(circle/p/color,sq/p/size,trg/p(color,size)),a(Info,foo,bar/baz),f/trg/p(type(byAngles,bySides)),_rev",
		"f(circle/p/color,sq/p/size,trg/p(color,size)),a(Info,foo,bar/baz),f/trg/p(type(byAngles,bySides),color),_rev",
		"f(circle/p/color,sq/p/size,trg/p/size),a(Info,foo,bar/baz),f/trg/p(type(byAngles,bySides),color),_rev",
	}

	for _, input := range inputs {
		assertPointers(t, input, output)
	}
}

func assertFunc(t *testing.T, testData fieldsTest) {
	assertPointers(t, testData.input, testData.output)
}

func assertPointers(t *testing.T, selector string, expPointers []string) {
	pointers, err := jsonutil.SelectorToJSONPointers(selector)
	require.NoError(t, err)

	assert.EqualValues(t, len(expPointers), len(pointers), selector, pointers)
	for _, expPointer := range expPointers {
		assert.Contains(t, pointers, expPointer, selector)
	}
}
