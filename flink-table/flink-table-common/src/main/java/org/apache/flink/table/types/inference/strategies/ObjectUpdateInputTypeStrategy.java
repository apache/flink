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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Input type strategy for the {@link BuiltInFunctionDefinitions#OBJECT_UPDATE} function.
 *
 * <p>This strategy validates the input arguments for updating existing fields in a structured type:
 *
 * <ul>
 *   <li>Ensures the function has an odd number of arguments (at least 3)
 *   <li>Validates the first argument is a structured type
 *   <li>Validates that key arguments are non-null string literals
 *   <li>Ensures field names are not repeated in the key-value pairs
 *   <li>Ensures field names are part of the structured type's attributes
 * </ul>
 *
 * <p>The expected signature is: {@code OBJECT_UPDATE(object, key1, value1, key2, value2, ...)}
 */
@Internal
public class ObjectUpdateInputTypeStrategy implements InputTypeStrategy {

    private static final ArgumentCount AT_LEAST_THREE_AND_ODD =
            new ArgumentCount() {
                @Override
                public boolean isValidCount(final int count) {
                    return count % 2 == 1;
                }

                @Override
                public Optional<Integer> getMinCount() {
                    return Optional.of(3);
                }

                @Override
                public Optional<Integer> getMaxCount() {
                    return Optional.empty();
                }
            };

    private static StructuredType validateObjectArgument(final DataType firstArgumentType) {
        final LogicalType firstArgumentLogicalType = firstArgumentType.getLogicalType();
        if (!firstArgumentLogicalType.is(LogicalTypeRoot.STRUCTURED_TYPE)) {
            throw new ValidationException(
                    String.format(
                            "The first argument must be a structured type, but was %s.",
                            firstArgumentLogicalType));
        }
        return (StructuredType) firstArgumentLogicalType;
    }

    private static void validateKeyValueArguments(
            final CallContext callContext,
            final List<DataType> argumentDataTypes,
            final StructuredType structuredType) {
        final Set<String> fieldNames = new HashSet<>();
        final Map<String, LogicalType> structuredTypeAttributeNameToLogicalType =
                structuredType.getAttributes().stream()
                        .collect(
                                Collectors.toMap(
                                        StructuredAttribute::getName,
                                        StructuredAttribute::getType));

        for (int i = 1; i < argumentDataTypes.size(); i += 2) {
            validateFieldNameArgument(
                    callContext,
                    argumentDataTypes,
                    i,
                    structuredTypeAttributeNameToLogicalType,
                    fieldNames);
        }
    }

    private static void validateFieldNameArgument(
            final CallContext callContext,
            final List<DataType> argumentDataTypes,
            final int pos,
            final Map<String, LogicalType> attributes,
            final Set<String> fieldNames) {
        final LogicalType fieldNameLogicalType = argumentDataTypes.get(pos).getLogicalType();
        if (!fieldNameLogicalType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            final String message =
                    String.format(
                            "The field key at position %d must be a non-null character string, but was %s.",
                            pos + 1, fieldNameLogicalType.asSummaryString());
            throw new ValidationException(message);
        }

        final String fieldName =
                callContext
                        .getArgumentValue(pos, String.class)
                        .orElseThrow(
                                () -> {
                                    final String message =
                                            String.format(
                                                    "The field key at position %d must be a non-null character string literal.",
                                                    pos + 1);
                                    return new ValidationException(message);
                                });

        // validate that the field name is not repeated
        if (!fieldNames.add(fieldName)) {
            final String message =
                    String.format(
                            "The field name '%s' at position %d is repeated.", fieldName, pos + 1);
            throw new ValidationException(message);
        }

        // validate that the field name is part of the structured type attributes
        if (!attributes.containsKey(fieldName)) {
            final String message =
                    String.format(
                            "The field name '%s' at position %d is not part of the structured type attributes. Available attributes: %s.",
                            fieldName, pos + 1, attributes.keySet());
            throw new ValidationException(message);
        }
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return AT_LEAST_THREE_AND_ODD;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            final CallContext callContext, final boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        try {
            final DataType firstArgumentType = argumentDataTypes.get(0);
            final StructuredType structuredType = validateObjectArgument(firstArgumentType);
            validateKeyValueArguments(callContext, argumentDataTypes, structuredType);
        } catch (ValidationException e) {
            return callContext.fail(throwOnFailure, e.getMessage());
        }

        return Optional.of(argumentDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(final FunctionDefinition definition) {
        // OBJECT_UPDATE expects: object, key1, value1, key2, value2, ...
        // OBJECT_UPDATE(<object>, <key>, <value> [, <key>, <value> , ...] )
        final List<Signature.Argument> arguments = new ArrayList<>();

        // object (required)
        final Argument classArgument = Argument.of("object", "STRUCTURED_TYPE");
        arguments.add(classArgument);

        // Key-value pairs (at least on pair, repeating)
        arguments.add(Signature.Argument.ofVarying("[STRING, ANY]+"));

        return List.of(Signature.of(arguments));
    }
}
