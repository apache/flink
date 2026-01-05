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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link TritonTypeMapper}. */
public class TritonTypeMapperTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testToTritonDataType() {
        assertEquals(TritonDataType.BOOL, TritonTypeMapper.toTritonDataType(new BooleanType()));
        assertEquals(TritonDataType.INT8, TritonTypeMapper.toTritonDataType(new TinyIntType()));
        assertEquals(TritonDataType.INT16, TritonTypeMapper.toTritonDataType(new SmallIntType()));
        assertEquals(TritonDataType.INT32, TritonTypeMapper.toTritonDataType(new IntType()));
        assertEquals(TritonDataType.INT64, TritonTypeMapper.toTritonDataType(new BigIntType()));
        assertEquals(TritonDataType.FP32, TritonTypeMapper.toTritonDataType(new FloatType()));
        assertEquals(TritonDataType.FP64, TritonTypeMapper.toTritonDataType(new DoubleType()));
        assertEquals(
                TritonDataType.BYTES,
                TritonTypeMapper.toTritonDataType(new VarCharType(VarCharType.MAX_LENGTH)));
    }

    @Test
    public void testToTritonDataTypeForArray() {
        // For arrays, returns the element type's Triton type
        assertEquals(
                TritonDataType.FP32,
                TritonTypeMapper.toTritonDataType(new ArrayType(new FloatType())));
        assertEquals(
                TritonDataType.INT32,
                TritonTypeMapper.toTritonDataType(new ArrayType(new IntType())));
    }

    @Test
    public void testSerializeScalarTypes() {
        // Test boolean
        RowData boolRow = GenericRowData.of(true);
        ArrayNode boolArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(boolRow, 0, new BooleanType(), boolArray);
        assertEquals(1, boolArray.size());
        assertEquals(true, boolArray.get(0).asBoolean());

        // Test int
        RowData intRow = GenericRowData.of(42);
        ArrayNode intArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(intRow, 0, new IntType(), intArray);
        assertEquals(1, intArray.size());
        assertEquals(42, intArray.get(0).asInt());

        // Test float
        RowData floatRow = GenericRowData.of(3.14f);
        ArrayNode floatArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(floatRow, 0, new FloatType(), floatArray);
        assertEquals(1, floatArray.size());
        assertEquals(3.14f, floatArray.get(0).floatValue(), 0.001f);

        // Test string
        RowData stringRow = GenericRowData.of(BinaryStringData.fromString("hello"));
        ArrayNode stringArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(
                stringRow, 0, new VarCharType(VarCharType.MAX_LENGTH), stringArray);
        assertEquals(1, stringArray.size());
        assertEquals("hello", stringArray.get(0).asText());
    }

    @Test
    public void testSerializeArrayType() {
        Float[] floatArray = new Float[] {1.0f, 2.0f, 3.0f};
        ArrayData arrayData = new GenericArrayData(floatArray);
        RowData rowData = GenericRowData.of(arrayData);

        ArrayNode jsonArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(
                rowData, 0, new ArrayType(new FloatType()), jsonArray);

        // Array should be flattened
        assertEquals(3, jsonArray.size());
        assertEquals(1.0f, jsonArray.get(0).floatValue(), 0.001f);
        assertEquals(2.0f, jsonArray.get(1).floatValue(), 0.001f);
        assertEquals(3.0f, jsonArray.get(2).floatValue(), 0.001f);
    }

    @Test
    public void testCalculateShape() {
        // Scalar type
        int[] scalarShape = TritonTypeMapper.calculateShape(new IntType(), 1);
        assertArrayEquals(new int[] {1}, scalarShape);

        // Array type
        int[] arrayShape = TritonTypeMapper.calculateShape(new ArrayType(new FloatType()), 1);
        assertArrayEquals(new int[] {1, -1}, arrayShape);

        // Batch size > 1
        int[] batchShape = TritonTypeMapper.calculateShape(new IntType(), 4);
        assertArrayEquals(new int[] {4}, batchShape);
    }

    @Test
    public void testDeserializeScalarTypes() {
        // Test int
        assertEquals(
                42,
                TritonTypeMapper.deserializeFromJson(objectMapper.valueToTree(42), new IntType()));

        // Test float
        Object floatResult =
                TritonTypeMapper.deserializeFromJson(
                        objectMapper.valueToTree(3.14f), new FloatType());
        assertEquals(3.14f, (Float) floatResult, 0.001f);

        // Test string
        Object stringResult =
                TritonTypeMapper.deserializeFromJson(
                        objectMapper.valueToTree("hello"), new VarCharType(VarCharType.MAX_LENGTH));
        assertEquals("hello", stringResult.toString());
    }

    @Test
    public void testDeserializeArrayType() {
        float[] floatArray = new float[] {1.0f, 2.0f, 3.0f};
        Object result =
                TritonTypeMapper.deserializeFromJson(
                        objectMapper.valueToTree(floatArray), new ArrayType(new FloatType()));

        ArrayData arrayData = (ArrayData) result;
        assertEquals(3, arrayData.size());
        assertEquals(1.0f, arrayData.getFloat(0), 0.001f);
        assertEquals(2.0f, arrayData.getFloat(1), 0.001f);
        assertEquals(3.0f, arrayData.getFloat(2), 0.001f);
    }

    @Test
    public void testSerializeNull() {
        GenericRowData nullRow = new GenericRowData(1);
        nullRow.setField(0, null);

        ArrayNode jsonArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(nullRow, 0, new IntType(), jsonArray);

        assertEquals(1, jsonArray.size());
        assertEquals(true, jsonArray.get(0).isNull());
    }
}
