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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Type strategy for the {@link BuiltInFunctionDefinitions#OBJECT_UPDATE} function.
 *
 * <p>This strategy infers the return type for the OBJECT_UPDATE function by:
 *
 * <ul>
 *   <li>Extracting the field definitions from the input structured type
 *   <li>Creating a new structured type with the same class but updating field types to match the
 *       types of the new values being assigned
 * </ul>
 *
 * <p>The return type preserves the original class and field names but may have different field
 * types than the input structured type, as field types are updated to match the types of the values
 * being assigned during the update operation.
 */
@Internal
public class ObjectUpdateTypeStrategy implements TypeStrategy {

    private static Field[] extractFields(
            final CallContext callContext,
            final List<DataType> argumentDataTypes,
            final StructuredType structuredType) {
        final List<String> existingFieldNames = LogicalTypeChecks.getFieldNames(structuredType);
        final List<LogicalType> existingFieldTypes =
                LogicalTypeChecks.getFieldTypes(structuredType);

        final Map<String, LogicalType> fieldNameToTypeIndex =
                IntStream.range(0, existingFieldNames.size())
                        .boxed()
                        .collect(
                                Collectors.toMap(existingFieldNames::get, existingFieldTypes::get));

        for (int i = 1; i < argumentDataTypes.size(); i += 2) {
            final String fieldNameToBeUpdated =
                    callContext
                            .getArgumentValue(i, String.class)
                            .orElseThrow(IllegalStateException::new);

            final LogicalType valueDataTypeToBeUpdated =
                    argumentDataTypes.get(i + 1).getLogicalType();

            final LogicalType valueLogicalType = fieldNameToTypeIndex.get(fieldNameToBeUpdated);

            if (!valueDataTypeToBeUpdated.equals(valueLogicalType)) {
                fieldNameToTypeIndex.put(fieldNameToBeUpdated, valueDataTypeToBeUpdated);
            }
        }

        return fieldNameToTypeIndex.entrySet().stream()
                .map(e -> toFieldType(e.getKey(), e.getValue()))
                .toArray(Field[]::new);
    }

    private static Field toFieldType(final String name, final LogicalType logicalType) {
        return DataTypes.FIELD(name, TypeConversions.fromLogicalToDataType(logicalType));
    }

    @Override
    public Optional<DataType> inferType(final CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final StructuredType structuredType =
                (StructuredType) argumentDataTypes.get(0).getLogicalType();

        final Field[] fields = extractFields(callContext, argumentDataTypes, structuredType);

        final String className =
                structuredType.getClassName().orElseThrow(IllegalStateException::new);
        final Optional<Class<?>> resolvedClass =
                StructuredType.resolveClass(
                        callContext.getDataTypeFactory().getClassLoader(), className);

        return Optional.of(
                resolvedClass
                        .map(clazz -> DataTypes.STRUCTURED(clazz, fields))
                        .orElse(DataTypes.STRUCTURED(className, fields)));
    }
}
