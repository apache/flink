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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Input type strategy for the {@code OBJECT_OF} function that validates argument types and counts.
 *
 * <p>This strategy validates the input arguments for the {@code OBJECT_OF} function, ensuring:
 *
 * <ul>
 *   <li>The argument count is odd (className + pairs of key-value arguments)
 *   <li>The first argument is a non-null STRING/VARCHAR representing the class name
 *   <li>All key arguments (odd positions after the first) are non-null STRING/VARCHAR types
 *   <li>Field names are unique across all key-value pairs
 *   <li>Value arguments (even positions after the first) can be any type
 * </ul>
 *
 * <p>The expected function signature is: {@code OBJECT_OF(className, key1, value1, key2, value2,
 * ...)}
 *
 * <p>Example valid calls:
 *
 * <ul>
 *   <li>{@code OBJECT_OF('com.example.User')} - empty object
 *   <li>{@code OBJECT_OF('com.example.User', 'name', 'Alice')} - single field
 *   <li>{@code OBJECT_OF('com.example.User', 'name', 'Alice', 'age', 30)} - multiple fields
 * </ul>
 *
 * @see org.apache.flink.table.functions.BuiltInFunctionDefinitions#OBJECT_OF
 * @see ObjectOfTypeStrategy
 */
@Internal
public class ObjectOfInputTypeStrategy implements InputTypeStrategy {

    private static final ArgumentCount AT_LEAST_ONE_ODD =
            new ArgumentCount() {
                @Override
                public boolean isValidCount(final int count) {
                    return count % 2 == 1;
                }

                @Override
                public Optional<Integer> getMinCount() {
                    return Optional.of(1);
                }

                @Override
                public Optional<Integer> getMaxCount() {
                    return Optional.empty();
                }
            };

    private static void validateClassArgument(
            final CallContext callContext, final DataType firstArgumentDataType) {
        final LogicalType classArgumentType = firstArgumentDataType.getLogicalType();

        if (!classArgumentType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            final String errorMessage =
                    String.format(
                            "The first argument must be a non-nullable character string representing the class name, but was %s.",
                            classArgumentType.asSummaryString());
            throw new ValidationException(errorMessage);
        }

        final Optional<String> argumentValue = callContext.getArgumentValue(0, String.class);
        if (argumentValue.isEmpty()) {
            final String errorMessage =
                    "The first argument must be a non-nullable character string literal representing the class name.";
            throw new ValidationException(errorMessage);
        }
    }

    private static void validateKeyArguments(
            final CallContext callContext, final List<DataType> argumentDataTypes) {
        final Set<String> fieldNames = new HashSet<>();
        for (int i = 1; i < argumentDataTypes.size(); i += 2) {
            final LogicalType fieldNameLogicalType = argumentDataTypes.get(i).getLogicalType();
            validateFieldNameArgument(callContext, i, fieldNameLogicalType, fieldNames);
        }
    }

    private static void validateFieldNameArgument(
            final CallContext callContext,
            final int pos,
            final LogicalType logicalType,
            final Set<String> fieldNames) {
        if (!logicalType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            final String message =
                    String.format(
                            "The field key at position %d must be a non-nullable character string, but was %s.",
                            pos + 1, logicalType.asSummaryString());
            throw new ValidationException(message);
        }

        final Optional<String> argumentValue = callContext.getArgumentValue(pos, String.class);
        if (argumentValue.isEmpty()) {
            final String message =
                    String.format(
                            "The field key at position %d must be a non-nullable character string literal.",
                            pos + 1);
            throw new ValidationException(message);
        }

        final String fieldName = argumentValue.orElseThrow(IllegalStateException::new);

        if (!fieldNames.add(fieldName)) {
            final String message =
                    String.format(
                            "The field name '%s' at position %d is repeated.", fieldName, pos + 1);
            throw new ValidationException(message);
        }
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return AT_LEAST_ONE_ODD;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            final CallContext callContext, final boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        try {
            validateClassArgument(callContext, argumentDataTypes.get(0));
            validateKeyArguments(callContext, argumentDataTypes);
        } catch (ValidationException e) {
            callContext.fail(throwOnFailure, e.getMessage(), argumentDataTypes);
        }

        return Optional.of(argumentDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(final FunctionDefinition definition) {
        // OBJECT_OF expects: className, key1, value1, key2, value2, ...
        // OBJECT_OF(<class name>, [<key>, <value> [, <key>, <value> , ...]] )
        final List<Signature.Argument> arguments = new ArrayList<>();

        // Class name (required)
        final Argument classArgument = Argument.of("class name", "STRING");
        arguments.add(classArgument);

        // Key-value pairs (optional, repeating)
        arguments.add(Signature.Argument.ofVarying("[STRING, ANY]*"));

        return List.of(Signature.of(arguments));
    }
}
