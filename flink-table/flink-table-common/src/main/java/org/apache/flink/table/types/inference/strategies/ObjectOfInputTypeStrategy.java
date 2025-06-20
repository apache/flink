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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
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
 *   <li>The first argument is a STRING/VARCHAR representing the class name
 *   <li>All key arguments (odd positions after the first) are STRING/VARCHAR types
 *   <li>Field names are unique across all key-value pairs
 *   <li>Value arguments (even positions after the first) can be any type
 * </ul>
 *
 * <p>The expected function signature is: {@code OBJECT_OF(className, key1, value1, key2, value2,
 * ...)}
 *
 * <p>Validation rules:
 *
 * <ul>
 *   <li>Minimum 1 argument (just the class name)
 *   <li>Odd total number of arguments (className + key-value pairs)
 *   <li>Keys must be string literals for field name extraction
 *   <li>No duplicate field names allowed
 * </ul>
 *
 * <p><b>Note: Users are responsible for providing a valid fully qualified class name that exists in
 * the classpath. The class name should follow Java naming conventions. While this strategy
 * validates the format and type of the class name argument, it does not verify the class existence
 * in the classpath. If an invalid or non-existent class name is provided, the function will fall
 * back to using Row.class as the type representation.</b>
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
public class ObjectOfInputTypeStrategy implements InputTypeStrategy {

    private static final ArgumentCount AT_LEAST_ONE_ODD =
            new ArgumentCount() {
                @Override
                public boolean isValidCount(int count) {
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

    private static void validateClassInput(
            final CallContext callContext, final List<DataType> argumentDataTypes) {
        final LogicalType classArgumentType = argumentDataTypes.get(0).getLogicalType();

        final String errorMessage =
                "The first argument must be a STRING/VARCHAR type representing the class name.";
        if (!classArgumentType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new ValidationException(errorMessage);
        }

        final Optional<String> className = callContext.getArgumentValue(0, String.class);
        className.orElseThrow(() -> new ValidationException(errorMessage));
    }

    private static void validateFieldNameInput(
            final CallContext callContext,
            final int idx,
            final LogicalType logicalType,
            final Set<String> fieldNames) {
        final int keyIndex = idx + 1;
        if (!logicalType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new ValidationException(
                    "The field key at position "
                            + keyIndex
                            + " must be a STRING/VARCHAR type, but was "
                            + logicalType.asSummaryString()
                            + ".");
        }
        final Optional<String> fieldName = callContext.getArgumentValue(idx, String.class);
        fieldName.ifPresent(
                name -> {
                    if (!fieldNames.add(name)) {
                        throw new ValidationException(
                                "The field name '"
                                        + name
                                        + "' at position "
                                        + keyIndex
                                        + " is repeated.");
                    }
                });
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return AT_LEAST_ONE_ODD;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            final CallContext callContext, final boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        validateClassInput(callContext, argumentDataTypes);

        final Set<String> fieldNames = new HashSet<>();
        for (int i = 1; i < argumentDataTypes.size(); i += 2) {
            final LogicalType fieldNameLogicalType = argumentDataTypes.get(i).getLogicalType();
            validateFieldNameInput(callContext, i, fieldNameLogicalType, fieldNames);
        }

        return Optional.of(argumentDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(final FunctionDefinition definition) {
        // OBJECT_OF expects: name, key1, value1, key2, value2, ...
        // OBJECT_OF(<name>, [<key>, <value> [, <key>, <value> , ...]] )
        final List<Signature.Argument> arguments = new ArrayList<>();

        // Class name (required)
        arguments.add(Signature.Argument.of("STRING"));

        // Key-value pairs (optional, repeating)
        arguments.add(Signature.Argument.ofVarying("[STRING, ANY]*"));

        return List.of(Signature.of(arguments));
    }
}
