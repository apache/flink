/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/** Test for {@link TritonTypeMapper}. */
class TritonTypeMapperTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testToTritonDataType() {

        assertThat(TritonTypeMapper.toTritonDataType(new BooleanType()))
                .isEqualTo(TritonDataType.BOOL);
        assertThat(TritonTypeMapper.toTritonDataType(new TinyIntType()))
                .isEqualTo(TritonDataType.INT8);
        assertThat(TritonTypeMapper.toTritonDataType(new SmallIntType()))
                .isEqualTo(TritonDataType.INT16);
        assertThat(TritonTypeMapper.toTritonDataType(new IntType()))
                .isEqualTo(TritonDataType.INT32);
        assertThat(TritonTypeMapper.toTritonDataType(new BigIntType()))
                .isEqualTo(TritonDataType.INT64);
        assertThat(TritonTypeMapper.toTritonDataType(new FloatType()))
                .isEqualTo(TritonDataType.FP32);
        assertThat(TritonTypeMapper.toTritonDataType(new DoubleType()))
                .isEqualTo(TritonDataType.FP64);
        assertThat(TritonTypeMapper.toTritonDataType(new VarCharType(VarCharType.MAX_LENGTH)))
                .isEqualTo(TritonDataType.BYTES);
    }

    @Test
    void testToTritonDataTypeForArray() {
        // For arrays, returns the element type's Triton type
        assertThat(TritonTypeMapper.toTritonDataType(new ArrayType(new FloatType())))
                .isEqualTo(TritonDataType.FP32);
        assertThat(TritonTypeMapper.toTritonDataType(new ArrayType(new IntType())))
                .isEqualTo(TritonDataType.INT32);
    }

    @Test
    void testSerializeScalarTypes() {
        // Test boolean
        RowData boolRow = GenericRowData.of(true);
        ArrayNode boolArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(boolRow, 0, new BooleanType(), boolArray);
        assertThat(boolArray).hasSize(1);
        assertThat(boolArray.get(0).asBoolean()).isTrue();

        // Test int
        RowData intRow = GenericRowData.of(42);
        ArrayNode intArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(intRow, 0, new IntType(), intArray);
        assertThat(intArray).hasSize(1);
        assertThat(intArray.get(0).asInt()).isEqualTo(42);

        // Test float
        RowData floatRow = GenericRowData.of(3.14f);
        ArrayNode floatArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(floatRow, 0, new FloatType(), floatArray);
        assertThat(floatArray).hasSize(1);
        assertThat(floatArray.get(0).floatValue()).isCloseTo(3.14f, within(0.001f));

        // Test string
        RowData stringRow = GenericRowData.of(BinaryStringData.fromString("hello"));
        ArrayNode stringArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(
                stringRow, 0, new VarCharType(VarCharType.MAX_LENGTH), stringArray);
        assertThat(stringArray).hasSize(1);
        assertThat(stringArray.get(0).asText()).isEqualTo("hello");
    }

    @Test
    void testSerializeArrayType() {
        Float[] floatArray = new Float[] {1.0f, 2.0f, 3.0f};
        ArrayData arrayData = new GenericArrayData(floatArray);
        RowData rowData = GenericRowData.of(arrayData);

        ArrayNode jsonArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(
                rowData, 0, new ArrayType(new FloatType()), jsonArray);

        // Array should be flattened
        assertThat(jsonArray).hasSize(3);
        assertThat(jsonArray.get(0).floatValue()).isCloseTo(1.0f, within(0.001f));
        assertThat(jsonArray.get(1).floatValue()).isCloseTo(2.0f, within(0.001f));
        assertThat(jsonArray.get(2).floatValue()).isCloseTo(3.0f, within(0.001f));
    }

    @Test
    void testCalculateShape() {
        // Scalar type
        int[] scalarShape = TritonTypeMapper.calculateShape(new IntType(), 1);
        assertThat(scalarShape).isEqualTo(new int[] {1});

        // Array type
        int[] arrayShape = TritonTypeMapper.calculateShape(new ArrayType(new FloatType()), 1);
        assertThat(arrayShape).isEqualTo(new int[] {1, -1});

        // Batch size > 1
        int[] batchShape = TritonTypeMapper.calculateShape(new IntType(), 4);
        assertThat(batchShape).isEqualTo(new int[] {4});
    }

    @Test
    void testDeserializeScalarTypes() {
        // Test int
        assertThat(
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.valueToTree(42), new IntType()))
                .isEqualTo(42);

        // Test float
        Object floatResult =
                TritonTypeMapper.deserializeFromJson(
                        objectMapper.valueToTree(3.14f), new FloatType());
        assertThat((Float) floatResult).isCloseTo(3.14f, within(0.001f));

        // Test string
        Object stringResult =
                TritonTypeMapper.deserializeFromJson(
                        objectMapper.valueToTree("hello"), new VarCharType(VarCharType.MAX_LENGTH));
        assertThat(stringResult).hasToString("hello");
    }

    @Test
    void testDeserializeArrayType() {
        float[] floatArray = new float[] {1.0f, 2.0f, 3.0f};
        Object result =
                TritonTypeMapper.deserializeFromJson(
                        objectMapper.valueToTree(floatArray), new ArrayType(new FloatType()));

        ArrayData arrayData = (ArrayData) result;
        assertThat(arrayData.size()).isEqualTo(3);
        assertThat(arrayData.getFloat(0)).isCloseTo(1.0f, within(0.001f));
        assertThat(arrayData.getFloat(1)).isCloseTo(2.0f, within(0.001f));
        assertThat(arrayData.getFloat(2)).isCloseTo(3.0f, within(0.001f));
    }

    @Test
    void testSerializeNull() {
        GenericRowData nullRow = new GenericRowData(1);
        nullRow.setField(0, null);

        ArrayNode jsonArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(nullRow, 0, new IntType(), jsonArray);

        assertThat(jsonArray).hasSize(1);
        assertThat(jsonArray.get(0).isNull()).isTrue();
    }

    @Test
    void testDeserializeArrayWithNullStringElement() throws Exception {
        ArrayData result =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[\"a\", null, \"b\"]"),
                                new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)));

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.isNullAt(1)).isTrue();
        assertThat(result.getString(0)).hasToString("a");
        assertThat(result.getString(2)).hasToString("b");
    }

    @Test
    void testDeserializeArrayWithNullNumericElements() throws Exception {
        ArrayData ints =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[1, null, 3]"),
                                new ArrayType(new IntType()));
        assertThat(ints.size()).isEqualTo(3);
        assertThat(ints.isNullAt(1)).isTrue();
        assertThat(ints.getInt(0)).isEqualTo(1);
        assertThat(ints.getInt(2)).isEqualTo(3);

        ArrayData longs =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[1, null]"),
                                new ArrayType(new BigIntType()));
        assertThat(longs.isNullAt(1)).isTrue();

        ArrayData doubles =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[1.5, null]"),
                                new ArrayType(new DoubleType()));
        assertThat(doubles.isNullAt(1)).isTrue();

        ArrayData floats =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[1.5, null]"),
                                new ArrayType(new FloatType()));
        assertThat(floats.isNullAt(1)).isTrue();

        ArrayData bytes =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[1, null]"),
                                new ArrayType(new TinyIntType()));
        assertThat(bytes.isNullAt(1)).isTrue();

        ArrayData shorts =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[1, null]"),
                                new ArrayType(new SmallIntType()));
        assertThat(shorts.isNullAt(1)).isTrue();
    }

    @Test
    void testDeserializeArrayWithNullBooleanElement() throws Exception {
        ArrayData result =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[true, null, false]"),
                                new ArrayType(new BooleanType()));

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.isNullAt(1)).isTrue();
        assertThat(result.getBoolean(0)).isTrue();
        assertThat(result.getBoolean(2)).isFalse();
    }

    @Test
    void testNullElementSurvivesSerializeDeserializeRoundTrip() throws Exception {
        ArrayType arrayType = new ArrayType(new VarCharType(VarCharType.MAX_LENGTH));
        RowData rowData =
                GenericRowData.of(
                        new GenericArrayData(
                                new Object[] {
                                    BinaryStringData.fromString("a"),
                                    null,
                                    BinaryStringData.fromString("b")
                                }));

        ArrayNode serialized = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(rowData, 0, arrayType, serialized);
        assertThat(serialized.get(1).isNull()).isTrue();

        ArrayData roundTripped =
                (ArrayData) TritonTypeMapper.deserializeFromJson(serialized, arrayType);

        assertThat(roundTripped.size()).isEqualTo(3);
        assertThat(roundTripped.isNullAt(1)).isTrue();
        assertThat(roundTripped.getString(0)).hasToString("a");
        assertThat(roundTripped.getString(2)).hasToString("b");
    }

    @Test
    void testDeserializeArrayRejectsNullForNotNullElementType() throws Exception {
        JsonNode dataNode = objectMapper.readTree("[1, null]");
        ArrayType notNullElements = new ArrayType(new IntType(false));

        assertThatThrownBy(() -> TritonTypeMapper.deserializeFromJson(dataNode, notNullElements))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NOT NULL");
    }

    @Test
    void testDeserializeArrayWithoutNullsIsUnchanged() throws Exception {
        ArrayData result =
                (ArrayData)
                        TritonTypeMapper.deserializeFromJson(
                                objectMapper.readTree("[1, 2, 3]"), new ArrayType(new IntType()));

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.isNullAt(0)).isFalse();
        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.getInt(1)).isEqualTo(2);
        assertThat(result.getInt(2)).isEqualTo(3);
    }

    @Test
    void testNullableArrayKeepsConcreteComponentType() throws Exception {
        // GenericArrayData requires a boxed array to carry its concrete component type; a plain
        // Object[] backing array is not a valid representation of ARRAY<INT>.
        assertThat(deserializeArray("[1, null, 3]", new IntType()).toObjectArray())
                .isInstanceOf(Integer[].class);
        assertThat(deserializeArray("[1, null]", new BigIntType()).toObjectArray())
                .isInstanceOf(Long[].class);
        assertThat(deserializeArray("[1, null]", new TinyIntType()).toObjectArray())
                .isInstanceOf(Byte[].class);
        assertThat(deserializeArray("[1, null]", new SmallIntType()).toObjectArray())
                .isInstanceOf(Short[].class);
        assertThat(deserializeArray("[1.5, null]", new FloatType()).toObjectArray())
                .isInstanceOf(Float[].class);
        assertThat(deserializeArray("[1.5, null]", new DoubleType()).toObjectArray())
                .isInstanceOf(Double[].class);
        assertThat(deserializeArray("[true, null]", new BooleanType()).toObjectArray())
                .isInstanceOf(Boolean[].class);
        assertThat(deserializeArray("[\"a\", null]", new VarCharType()).toObjectArray())
                .isInstanceOf(StringData[].class);
    }

    @Test
    void testNullableArraySurvivesExternalConversion() throws Exception {
        // ArrayObjectArrayConverter#toExternal returns the backing array of a GenericArrayData
        // directly, so an Object[] backing array would fail the cast to Integer[] below.
        ArrayData ints = deserializeArray("[1, null, 3]", new IntType());

        DataStructureConverter<Object, Object> converter =
                DataStructureConverters.getConverter(DataTypes.ARRAY(DataTypes.INT()));
        converter.open(TritonTypeMapperTest.class.getClassLoader());

        Integer[] external = (Integer[]) converter.toExternal(ints);
        assertThat(external).containsExactly(1, null, 3);
    }

    private GenericArrayData deserializeArray(String json, LogicalType elementType)
            throws Exception {
        return (GenericArrayData)
                TritonTypeMapper.deserializeFromJson(
                        objectMapper.readTree(json), new ArrayType(elementType));
    }
}
