// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package marshal

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/nomdl"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestMarshalTypeType(tt *testing.T) {
	t := func(exp *types.Type, ptr interface{}) {
		p := reflect.ValueOf(ptr)
		assert.NotEqual(tt, reflect.Ptr, p.Type().Kind())
		actual, err := MarshalType(types.Format_7_18, p.Interface())
		assert.NoError(tt, err)
		assert.NotNil(tt, actual, "%#v", p.Interface())
		assert.True(tt, exp.Equals(actual))
	}

	t(types.FloaTType, float32(0))
	t(types.FloaTType, float64(0))
	t(types.FloaTType, int(0))
	t(types.FloaTType, int16(0))
	t(types.FloaTType, int32(0))
	t(types.FloaTType, int64(0))
	t(types.FloaTType, int8(0))
	t(types.FloaTType, uint(0))
	t(types.FloaTType, uint16(0))
	t(types.FloaTType, uint32(0))
	t(types.FloaTType, uint64(0))
	t(types.FloaTType, uint8(0))

	t(types.BoolType, true)
	t(types.StringType, "hi")

	var l []int
	t(mustType(types.MakeListType(types.FloaTType)), l)

	var m map[uint32]string
	t(mustType(types.MakeMapType(types.FloaTType, types.StringType)), m)

	t(mustType(types.MakeListType(types.ValueType)), types.List{})
	t(mustType(types.MakeSetType(types.ValueType)), types.Set{})
	t(mustType(types.MakeMapType(types.ValueType, types.ValueType)), types.Map{})
	t(mustType(types.MakeRefType(types.ValueType)), types.Ref{})

	type TestStruct struct {
		Str string
		Num float64
	}
	var str TestStruct
	t(mustType(types.MakeStructTypeFromFields("TestStruct", types.FieldMap{
		"str": types.StringType,
		"num": types.FloaTType,
	})), str)

	// Same again to test caching
	t(mustType(types.MakeStructTypeFromFields("TestStruct", types.FieldMap{
		"str": types.StringType,
		"num": types.FloaTType,
	})), str)

	anonStruct := struct {
		B bool
	}{
		true,
	}
	t(mustType(types.MakeStructTypeFromFields("", types.FieldMap{
		"b": types.BoolType,
	})), anonStruct)

	type TestNestedStruct struct {
		A []int16
		B TestStruct
		C float64
	}
	var nestedStruct TestNestedStruct
	t(mustType(types.MakeStructTypeFromFields("TestNestedStruct", types.FieldMap{
		"a": mustType(types.MakeListType(types.FloaTType)),
		"b": mustType(types.MakeStructTypeFromFields("TestStruct", types.FieldMap{
			"str": types.StringType,
			"num": types.FloaTType,
		})),
		"c": types.FloaTType,
	})), nestedStruct)

	type testStruct struct {
		Str string
		Num float64
	}
	var ts testStruct
	t(mustType(types.MakeStructTypeFromFields("TestStruct", types.FieldMap{
		"str": types.StringType,
		"num": types.FloaTType,
	})), ts)
}

//
func assertMarshalTypeErrorMessage(t *testing.T, v interface{}, expectedMessage string) {
	_, err := MarshalType(types.Format_7_18, v)
	assert.Error(t, err)
	assert.Equal(t, expectedMessage, err.Error())
}

func TestMarshalTypeInvalidTypes(t *testing.T) {
	assertMarshalTypeErrorMessage(t, make(chan int), "Type is not supported, type: chan int")
}

func TestMarshalTypeEmbeddedStruct(t *testing.T) {
	assert := assert.New(t)

	type EmbeddedStruct struct {
		B bool
	}
	type TestStruct struct {
		EmbeddedStruct
		A int
	}

	var s TestStruct
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)

	assert.True(mustType(types.MakeStructTypeFromFields("TestStruct", types.FieldMap{
		"a": types.FloaTType,
		"b": types.BoolType,
	})).Equals(typ))
}

func TestMarshalTypeEmbeddedStructSkip(t *testing.T) {
	assert := assert.New(t)

	type EmbeddedStruct struct {
		B bool
	}
	type TestStruct struct {
		EmbeddedStruct `noms:"-"`
		A              int
	}

	var s TestStruct
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)

	assert.True(mustType(types.MakeStructTypeFromFields("TestStruct", types.FieldMap{
		"a": types.FloaTType,
	})).Equals(typ))
}

func TestMarshalTypeEmbeddedStructNamed(t *testing.T) {
	assert := assert.New(t)

	type EmbeddedStruct struct {
		B bool
	}
	type TestStruct struct {
		EmbeddedStruct `noms:"em"`
		A              int
	}

	var s TestStruct
	typ := mustMarshalType(types.Format_7_18, s)

	assert.True(mustType(types.MakeStructTypeFromFields("TestStruct", types.FieldMap{
		"a": types.FloaTType,
		"em": mustType(types.MakeStructTypeFromFields("EmbeddedStruct", types.FieldMap{
			"b": types.BoolType,
		})),
	})).Equals(typ))
}

func TestMarshalTypeEncodeNonExportedField(t *testing.T) {
	type TestStruct struct {
		x int
	}
	assertMarshalTypeErrorMessage(t, TestStruct{1}, "Non exported fields are not supported, type: marshal.TestStruct")
}

func TestMarshalTypeEncodeTaggingSkip(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Abc int `noms:"-"`
		Def bool
	}
	var s S
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)
	assert.True(mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"def": types.BoolType,
	})).Equals(typ))
}

func TestMarshalTypeNamedFields(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Aaa int  `noms:"a"`
		Bbb bool `noms:"B"`
		Ccc string
	}
	var s S
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)
	assert.True(mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"a":   types.FloaTType,
		"B":   types.BoolType,
		"ccc": types.StringType,
	})).Equals(typ))
}

func TestMarshalTypeInvalidNamedFields(t *testing.T) {
	type S struct {
		A int `noms:"1a"`
	}
	var s S
	assertMarshalTypeErrorMessage(t, s, "Invalid struct field name: 1a")
}

func TestMarshalTypeOmitEmpty(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		String string `noms:",omitempty"`
	}
	var s S
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)
	assert.True(mustType(types.MakeStructType("S", types.StructField{Name: "string", Type: types.StringType, Optional: true})).Equals(typ))
}

func SkipExampleMarshalType() {
	type Person struct {
		Given  string
		Female bool
	}
	var person Person
	personNomsType, err := MarshalType(types.Format_7_18, person)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(personNomsType.Describe(context.Background()))
	// Output: Struct Person {
	//   female: Bool,
	//   given: String,
	// }
}

func TestMarshalTypeSlice(t *testing.T) {
	assert := assert.New(t)

	s := []string{"a", "b", "c"}
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)
	assert.True(mustType(types.MakeListType(types.StringType)).Equals(typ))
}

func TestMarshalTypeArray(t *testing.T) {
	assert := assert.New(t)

	a := [3]int{1, 2, 3}
	typ, err := MarshalType(types.Format_7_18, a)
	assert.NoError(err)
	assert.True(mustType(types.MakeListType(types.FloaTType)).Equals(typ))
}

func TestMarshalTypeStructWithSlice(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		List []int
	}
	var s S
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)
	assert.True(mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"list": mustType(types.MakeListType(types.FloaTType)),
	})).Equals(typ))
}

func TestMarshalTypeRecursive(t *testing.T) {
	assert := assert.New(t)

	type Node struct {
		Value    int
		Children []Node
	}
	var n Node
	typ, err := MarshalType(types.Format_7_18, n)
	assert.NoError(err)

	typ2 := mustType(types.MakeStructType("Node",
		types.StructField{
			Name: "children",
			Type: mustType(types.MakeListType(types.MakeCycleType("Node"))),
		},
		types.StructField{
			Name: "value",
			Type: types.FloaTType,
		},
	))
	assert.True(typ2.Equals(typ))
}

func TestMarshalTypeMap(t *testing.T) {
	assert := assert.New(t)

	var m map[string]int
	typ, err := MarshalType(types.Format_7_18, m)
	assert.NoError(err)
	assert.True(mustType(types.MakeMapType(types.StringType, types.FloaTType)).Equals(typ))

	type S struct {
		N string
	}

	var m2 map[S]bool
	typ, err = MarshalType(types.Format_7_18, m2)
	assert.NoError(err)
	assert.True(mustType(types.MakeMapType(
		mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
			"n": types.StringType,
		})),
		types.BoolType)).Equals(typ))
}

func TestMarshalTypeSet(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		A map[int]struct{} `noms:",set"`
		B map[int]struct{}
		C map[int]string      `noms:",set"`
		D map[string]struct{} `noms:",set"`
		E map[string]struct{}
		F map[string]int `noms:",set"`
		G []int          `noms:",set"`
		H string         `noms:",set"`
	}
	var s S
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)

	emptyStructType := mustType(types.MakeStructTypeFromFields("", types.FieldMap{}))

	assert.True(mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"a": mustType(types.MakeSetType(types.FloaTType)),
		"b": mustType(types.MakeMapType(types.FloaTType, emptyStructType)),
		"c": mustType(types.MakeMapType(types.FloaTType, types.StringType)),
		"d": mustType(types.MakeSetType(types.StringType)),
		"e": mustType(types.MakeMapType(types.StringType, emptyStructType)),
		"f": mustType(types.MakeMapType(types.StringType, types.FloaTType)),
		"g": mustType(types.MakeSetType(types.FloaTType)),
		"h": types.StringType,
	})).Equals(typ))
}

func TestEncodeTypeOpt(t *testing.T) {
	assert := assert.New(t)

	tc := []struct {
		in       interface{}
		opt      Opt
		wantType *types.Type
	}{
		{
			[]string{},
			Opt{},
			mustType(types.MakeListType(types.StringType)),
		},
		{
			[]string{},
			Opt{Set: true},
			mustType(types.MakeSetType(types.StringType)),
		},
		{
			map[string]struct{}{},
			Opt{},
			mustType(types.MakeMapType(types.StringType, mustType(types.MakeStructType("")))),
		},
		{
			map[string]struct{}{},
			Opt{Set: true},
			mustType(types.MakeSetType(types.StringType)),
		},
	}

	for _, t := range tc {
		r, err := MarshalTypeOpt(types.Format_7_18, t.in, t.opt)
		assert.True(t.wantType.Equals(r))
		assert.Nil(err)
	}
}

func TestMarshalTypeSetWithTags(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		A map[int]struct{} `noms:"foo,set"`
		B map[int]struct{} `noms:",omitempty,set"`
		C map[int]struct{} `noms:"bar,omitempty,set"`
	}

	var s S
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)
	assert.True(mustType(types.MakeStructType("S",
		types.StructField{Name: "foo", Type: mustType(types.MakeSetType(types.FloaTType)), Optional: false},
		types.StructField{Name: "b", Type: mustType(types.MakeSetType(types.FloaTType)), Optional: true},
		types.StructField{Name: "bar", Type: mustType(types.MakeSetType(types.FloaTType)), Optional: true},
	)).Equals(typ))
}

func TestMarshalTypeInvalidTag(t *testing.T) {

	type S struct {
		F string `noms:",omitEmpty"`
	}
	var s S
	_, err := MarshalType(types.Format_7_18, s)
	assert.Error(t, err)
	assert.Equal(t, `Unrecognized tag: omitEmpty`, err.Error())
}

func TestMarshalTypeCanSkipUnexportedField(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Abc         int
		notExported bool `noms:"-"`
	}
	var s S
	assert.False(s.notExported) // here to remove compiler warning about notExported not being used.

	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)
	assert.True(mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"abc": types.FloaTType,
	})).Equals(typ))
}

func TestMarshalTypeOriginal(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Foo int          `noms:",omitempty"`
		Bar types.Struct `noms:",original"`
	}

	var s S
	typ, err := MarshalType(types.Format_7_18, s)
	assert.NoError(err)
	assert.True(mustType(types.MakeStructType("S",
		types.StructField{Name: "foo", Type: types.FloaTType, Optional: true},
	)).Equals(typ))
}

func TestMarshalTypeNomsTypes(t *testing.T) {
	assert := assert.New(t)

	type S struct {
		Blob   types.Blob
		Bool   types.Bool
		Number types.Float
		String types.String
		Type   *types.Type
	}
	var s S
	assert.True(mustMarshalType(types.Format_7_18, s).Equals(
		mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
			"blob":   types.BlobType,
			"bool":   types.BoolType,
			"number": types.FloaTType,
			"string": types.StringType,
			"type":   types.TypeType,
		})),
	))
}

func (t primitiveType) MarshalNomsType() (*types.Type, error) {
	return types.FloaTType, nil
}

func TestTypeMarshalerPrimitiveType(t *testing.T) {
	assert := assert.New(t)

	var u primitiveType
	typ := mustMarshalType(types.Format_7_18, u)
	assert.Equal(types.FloaTType, typ)
}

func (u primitiveSliceType) MarshalNomsType() (*types.Type, error) {
	return types.StringType, nil
}

func TestTypeMarshalerPrimitiveSliceType(t *testing.T) {
	assert := assert.New(t)

	var u primitiveSliceType
	typ := mustMarshalType(types.Format_7_18, u)
	assert.Equal(types.StringType, typ)
}

func (u primitiveMapType) MarshalNomsType() (*types.Type, error) {
	return mustType(types.MakeSetType(types.StringType)), nil
}

func TestTypeMarshalerPrimitiveMapType(t *testing.T) {
	assert := assert.New(t)

	var u primitiveMapType
	typ := mustMarshalType(types.Format_7_18, u)
	assert.Equal(mustType(types.MakeSetType(types.StringType)), typ)
}

func TestTypeMarshalerPrimitiveStructTypeNoMarshalNomsType(t *testing.T) {
	assert := assert.New(t)

	var u primitiveStructType
	_, err := MarshalType(types.Format_7_18, u)
	assert.Error(err)
	assert.Equal("cannot marshal type which implements marshal.Marshaler, perhaps implement marshal.TypeMarshaler for marshal.primitiveStructType", err.Error())
}

func (u builtinType) MarshalNomsType() (*types.Type, error) {
	return types.StringType, nil
}

func TestTypeMarshalerBuiltinType(t *testing.T) {
	assert := assert.New(t)

	var u builtinType
	typ := mustMarshalType(types.Format_7_18, u)
	assert.Equal(types.StringType, typ)
}

func (u wrappedMarshalerType) MarshalNomsType() (*types.Type, error) {
	return types.FloaTType, nil
}

func TestTypeMarshalerWrapperMarshalerType(t *testing.T) {
	assert := assert.New(t)

	var u wrappedMarshalerType
	typ := mustMarshalType(types.Format_7_18, u)
	assert.Equal(types.FloaTType, typ)
}

func (u returnsMarshalerError) MarshalNomsType() (*types.Type, error) {
	return nil, errors.New("expected error")
}

func (u returnsMarshalerNil) MarshalNomsType() (*types.Type, error) {
	return nil, nil
}

func TestTypeMarshalerErrors(t *testing.T) {
	assert := assert.New(t)

	expErr := errors.New("expected error")
	var m1 returnsMarshalerError
	_, actErr := MarshalType(types.Format_7_18, m1)
	assert.Equal(&marshalNomsError{expErr}, actErr)

	var m2 returnsMarshalerNil
	_, err := MarshalType(types.Format_7_18, m2)
	assert.Error(err)
}

func TestMarshalTypeStructName(t *testing.T) {
	assert := assert.New(t)

	var ts TestStructWithNameImpl
	typ := mustMarshalType(types.Format_7_18, ts)
	assert.True(mustType(types.MakeStructType("A", types.StructField{Name: "x", Type: types.FloaTType, Optional: false})).Equals(typ), mustString(typ.Describe(context.Background())))
}

func TestMarshalTypeStructName2(t *testing.T) {
	assert := assert.New(t)

	var ts TestStructWithNameImpl2
	typ := mustMarshalType(types.Format_7_18, ts)
	assert.True(mustType(types.MakeStructType("", types.StructField{Name: "x", Type: types.FloaTType, Optional: false})).Equals(typ), mustString(typ.Describe(context.Background())))
}

type OutPhoto struct {
	Faces             []OutFace `noms:",set"`
	SomeOtherFacesSet []OutFace `noms:",set"`
}

type OutFace struct {
	Blob types.Ref
}

func (f OutFace) MarshalNomsStructName() string {
	return "Face"
}

func TestMarshalTypeOutface(t *testing.T) {

	typ := mustMarshalType(types.Format_7_18, OutPhoto{})
	expectedType := nomdl.MustParseType(`Struct OutPhoto {
          faces: Set<Struct Face {
            blob: Ref<Value>,
          }>,
          someOtherFacesSet: Set<Cycle<Face>>,
        }`)
	assert.True(t, typ.Equals(expectedType))
}
