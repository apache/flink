/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
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
import com.fasterxml.jackson.databind.node.ArrayNode;

/** Utility class for mapping between Flink logical types and Triton data types. */
public class TritonTypeMapper {

    /**
     * Maps a Flink LogicalType to the corresponding Triton data type.
     *
     * @param logicalType The Flink logical type
     * @return The corresponding Triton data type
     * @throws IllegalArgumentException if the type is not supported
     */
    public static TritonDataType toTritonDataType(LogicalType logicalType) {
        if (logicalType instanceof BooleanType) {
            return TritonDataType.BOOL;
        } else if (logicalType instanceof TinyIntType) {
            return TritonDataType.INT8;
        } else if (logicalType instanceof SmallIntType) {
            return TritonDataType.INT16;
        } else if (logicalType instanceof IntType) {
            return TritonDataType.INT32;
        } else if (logicalType instanceof BigIntType) {
            return TritonDataType.INT64;
        } else if (logicalType instanceof FloatType) {
            return TritonDataType.FP32;
        } else if (logicalType instanceof DoubleType) {
            return TritonDataType.FP64;
        } else if (logicalType instanceof VarCharType) {
            return TritonDataType.BYTES;
        } else if (logicalType instanceof ArrayType) {
            // For arrays, we map the element type
            ArrayType arrayType = (ArrayType) logicalType;
            return toTritonDataType(arrayType.getElementType());
        } else {
            throw new IllegalArgumentException("Unsupported Flink type for Triton: " + logicalType);
        }
    }

    /**
     * Serializes Flink RowData field value to JSON array for Triton request.
     *
     * @param rowData The row data
     * @param fieldIndex The field index
     * @param logicalType The logical type of the field
     * @param dataArray The JSON array to add data to
     */
    public static void serializeToJsonArray(
            RowData rowData, int fieldIndex, LogicalType logicalType, ArrayNode dataArray) {
        if (rowData.isNullAt(fieldIndex)) {
            dataArray.addNull();
            return;
        }

        if (logicalType instanceof BooleanType) {
            dataArray.add(rowData.getBoolean(fieldIndex));
        } else if (logicalType instanceof TinyIntType) {
            dataArray.add(rowData.getByte(fieldIndex));
        } else if (logicalType instanceof SmallIntType) {
            dataArray.add(rowData.getShort(fieldIndex));
        } else if (logicalType instanceof IntType) {
            dataArray.add(rowData.getInt(fieldIndex));
        } else if (logicalType instanceof BigIntType) {
            dataArray.add(rowData.getLong(fieldIndex));
        } else if (logicalType instanceof FloatType) {
            dataArray.add(rowData.getFloat(fieldIndex));
        } else if (logicalType instanceof DoubleType) {
            dataArray.add(rowData.getDouble(fieldIndex));
        } else if (logicalType instanceof VarCharType) {
            dataArray.add(rowData.getString(fieldIndex).toString());
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            ArrayData arrayData = rowData.getArray(fieldIndex);
            serializeArrayToJsonArray(arrayData, arrayType.getElementType(), dataArray);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Flink type for serialization: " + logicalType);
        }
    }

    /**
     * Serializes Flink ArrayData to JSON array (flattened).
     *
     * @param arrayData The array data
     * @param elementType The element type
     * @param targetArray The JSON array to add data to
     */
    private static void serializeArrayToJsonArray(
            ArrayData arrayData, LogicalType elementType, ArrayNode targetArray) {
        int size = arrayData.size();
        for (int i = 0; i < size; i++) {
            if (arrayData.isNullAt(i)) {
                targetArray.addNull();
                continue;
            }

            if (elementType instanceof BooleanType) {
                targetArray.add(arrayData.getBoolean(i));
            } else if (elementType instanceof TinyIntType) {
                targetArray.add(arrayData.getByte(i));
            } else if (elementType instanceof SmallIntType) {
                targetArray.add(arrayData.getShort(i));
            } else if (elementType instanceof IntType) {
                targetArray.add(arrayData.getInt(i));
            } else if (elementType instanceof BigIntType) {
                targetArray.add(arrayData.getLong(i));
            } else if (elementType instanceof FloatType) {
                targetArray.add(arrayData.getFloat(i));
            } else if (elementType instanceof DoubleType) {
                targetArray.add(arrayData.getDouble(i));
            } else if (elementType instanceof VarCharType) {
                targetArray.add(arrayData.getString(i).toString());
            } else {
                throw new IllegalArgumentException(
                        "Unsupported array element type: " + elementType);
            }
        }
    }

    /**
     * Deserializes JSON data to Flink object based on logical type.
     *
     * @param dataNode The JSON node containing the data
     * @param logicalType The target logical type
     * @return The deserialized object
     */
    public static Object deserializeFromJson(JsonNode dataNode, LogicalType logicalType) {
        if (dataNode == null || dataNode.isNull()) {
            return null;
        }

        if (logicalType instanceof BooleanType) {
            return dataNode.asBoolean();
        } else if (logicalType instanceof TinyIntType) {
            return (byte) dataNode.asInt();
        } else if (logicalType instanceof SmallIntType) {
            return (short) dataNode.asInt();
        } else if (logicalType instanceof IntType) {
            return dataNode.asInt();
        } else if (logicalType instanceof BigIntType) {
            return dataNode.asLong();
        } else if (logicalType instanceof FloatType) {
            // Use floatValue() to properly handle the conversion
            if (dataNode.isNumber()) {
                return dataNode.floatValue();
            } else {
                return (float) dataNode.asDouble();
            }
        } else if (logicalType instanceof DoubleType) {
            return dataNode.asDouble();
        } else if (logicalType instanceof VarCharType) {
            return BinaryStringData.fromString(dataNode.asText());
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            return deserializeArrayFromJson(dataNode, arrayType.getElementType());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Flink type for deserialization: " + logicalType);
        }
    }

    /**
     * Deserializes JSON array to Flink ArrayData.
     *
     * @param dataNode The JSON array node
     * @param elementType The element type
     * @return The deserialized ArrayData
     */
    private static ArrayData deserializeArrayFromJson(JsonNode dataNode, LogicalType elementType) {
        if (!dataNode.isArray()) {
            throw new IllegalArgumentException(
                    "Expected JSON array but got: " + dataNode.getNodeType());
        }

        int size = dataNode.size();

        // Handle different element types with appropriate array types
        if (elementType instanceof BooleanType) {
            boolean[] array = new boolean[size];
            int i = 0;
            for (JsonNode element : dataNode) {
                array[i++] = element.asBoolean();
            }
            return new GenericArrayData(array);
        } else if (elementType instanceof TinyIntType) {
            byte[] array = new byte[size];
            int i = 0;
            for (JsonNode element : dataNode) {
                array[i++] = (byte) element.asInt();
            }
            return new GenericArrayData(array);
        } else if (elementType instanceof SmallIntType) {
            short[] array = new short[size];
            int i = 0;
            for (JsonNode element : dataNode) {
                array[i++] = (short) element.asInt();
            }
            return new GenericArrayData(array);
        } else if (elementType instanceof IntType) {
            int[] array = new int[size];
            int i = 0;
            for (JsonNode element : dataNode) {
                array[i++] = element.asInt();
            }
            return new GenericArrayData(array);
        } else if (elementType instanceof BigIntType) {
            long[] array = new long[size];
            int i = 0;
            for (JsonNode element : dataNode) {
                array[i++] = element.asLong();
            }
            return new GenericArrayData(array);
        } else if (elementType instanceof FloatType) {
            float[] array = new float[size];
            int i = 0;
            for (JsonNode element : dataNode) {
                array[i++] = element.isNumber() ? element.floatValue() : (float) element.asDouble();
            }
            return new GenericArrayData(array);
        } else if (elementType instanceof DoubleType) {
            double[] array = new double[size];
            int i = 0;
            for (JsonNode element : dataNode) {
                array[i++] = element.asDouble();
            }
            return new GenericArrayData(array);
        } else if (elementType instanceof VarCharType) {
            BinaryStringData[] array = new BinaryStringData[size];
            int i = 0;
            for (JsonNode element : dataNode) {
                array[i++] = BinaryStringData.fromString(element.asText());
            }
            return new GenericArrayData(array);
        } else {
            throw new IllegalArgumentException("Unsupported array element type: " + elementType);
        }
    }

    /**
     * Calculates the shape dimensions for the input data.
     *
     * @param logicalType The logical type
     * @param batchSize The batch size
     * @return Array of dimensions
     */
    public static int[] calculateShape(LogicalType logicalType, int batchSize) {
        if (logicalType instanceof ArrayType) {
            // For arrays, we need to know the array size at runtime
            // Return shape with batch size and -1 for dynamic dimension
            return new int[] {batchSize, -1};
        } else {
            // For scalar types, shape is just the batch size
            return new int[] {batchSize};
        }
    }

    /**
     * Calculates the shape dimensions for the input data based on actual row data.
     *
     * @param logicalType The logical type
     * @param batchSize The batch size
     * @param rowData The actual row data
     * @param fieldIndex The field index in the row
     * @return Array of dimensions
     */
    public static int[] calculateShape(
            LogicalType logicalType, int batchSize, RowData rowData, int fieldIndex) {
        if (logicalType instanceof ArrayType) {
            // For arrays, calculate actual size from the data
            if (rowData.isNullAt(fieldIndex)) {
                // Null array - return shape [batchSize, 0]
                return new int[] {batchSize, 0};
            }
            ArrayData arrayData = rowData.getArray(fieldIndex);
            int arraySize = arrayData.size();
            return new int[] {batchSize, arraySize};
        } else {
            // For scalar types, shape is just the batch size
            return new int[] {batchSize};
        }
    }
}
