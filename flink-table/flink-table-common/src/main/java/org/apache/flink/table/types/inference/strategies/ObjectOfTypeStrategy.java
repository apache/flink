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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Type strategy for the {@code OBJECT_OF} function that infers the output type as a structured
 * type.
 *
 * <p>This strategy creates a {@link DataTypes#STRUCTURED} type based on the provided class name and
 * key-value pairs. The function signature is: {@code OBJECT_OF(className, key1, value1, key2,
 * value2, ...)}
 *
 * <p>The strategy performs the following operations:
 *
 * <ul>
 *   <li>Extracts the class name from the first argument (must be a string literal)
 *   <li>Processes key-value pairs starting from the second argument
 *   <li>Extracts field names from odd-positioned arguments (indices 1, 3, 5, ...)
 *   <li>Normalizes field types from even-positioned arguments (indices 2, 4, 6, ...)
 *   <li>Creates a structured type with the given class name and normalized fields
 * </ul>
 *
 * <p>Field type normalization includes:
 *
 * <ul>
 *   <li>Converting CHARACTER_STRING types (CHAR, VARCHAR) to nullable VARCHAR (STRING)
 *   <li>Making all field types nullable for flexibility in structured types
 * </ul>
 *
 * <p>The strategy returns {@code Optional.empty()} if the class name argument is not available as a
 * literal value (e.g., during type inference testing scenarios).
 *
 * <p><b>Note: Users are responsible for providing a valid fully qualified class name that exists in
 * the classpath. The class name should follow Java naming conventions. If an invalid or
 * non-existent class name is provided, the function will fall back to using Row.class as the type
 * representation.</b>
 *
 * <p><b>Examples:</b>
 *
 * <ul>
 *   <li>{@code OBJECT_OF('com.example.User', 'name', 'Alice', 'age', 30)} → {@code
 *       STRUCTURED<com.example.User>(name STRING, age INT)}
 *   <li>{@code OBJECT_OF('com.example.Point', 'x', 1.5, 'y', 2.0)} → {@code
 *       STRUCTURED<com.example.Point>(x DOUBLE, y DOUBLE)}
 * </ul>
 *
 * <p><b>Implementation Notes:</b>
 *
 * <ul>
 *   <li>Field names must be available as string literals during type inference
 *   <li>The class name is used for type identification but the runtime representation is RowData
 *   <li>Uses {@link IntStream} for efficient processing of key-value pairs
 * </ul>
 *
 * @see org.apache.flink.table.functions.BuiltInFunctionDefinitions#OBJECT_OF
 * @see ObjectOfInputTypeStrategy
 */
public class ObjectOfTypeStrategy implements TypeStrategy {

    private static DataType toStructuredType(
            final String className, final CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final DataTypes.Field[] fields =
                IntStream.iterate(1, i -> i < argumentDataTypes.size(), i -> i + 2)
                        .mapToObj(keyIdx -> toFieldDataType(callContext, keyIdx))
                        .toArray(DataTypes.Field[]::new);

        return DataTypes.STRUCTURED(className, fields);
    }

    private static Field toFieldDataType(final CallContext callContext, final int keyIdx) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();

        final String fieldName =
                callContext
                        .getArgumentValue(keyIdx, String.class)
                        .orElseThrow(IllegalStateException::new);

        final DataType fieldValueType = argumentDataTypes.get(keyIdx + 1);
        final DataType normalizedFieldValueType = normalizeFieldType(fieldValueType);
        return DataTypes.FIELD(fieldName, normalizedFieldValueType);
    }

    /**
     * Normalizes field types for structured types: - Converts CHARACTER_STRING types (CHAR,
     * VARCHAR) to VARCHAR (STRING) - Makes all types nullable for flexibility in structured types.
     */
    private static DataType normalizeFieldType(DataType fieldType) {
        final LogicalType logicalType = fieldType.getLogicalType();

        if (logicalType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return DataTypes.STRING().nullable();
        }

        return TypeConversions.fromLogicalToDataType(logicalType.copy(true));
    }

    @Override
    public Optional<DataType> inferType(final CallContext callContext) {
        final Optional<String> argumentValue = callContext.getArgumentValue(0, String.class);

        return argumentValue.map(className -> toStructuredType(className, callContext));
    }
}
