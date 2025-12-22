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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.inference.strategies.StrategyUtils.findDataType;

/**
 * Strategy for an argument that corresponds to an {@code ARRAY} with specified element {@link
 * LogicalTypeRoot} and nullability.
 *
 * <p>Implicit casts for element will be inserted if possible.
 */
@Internal
public class ArrayOfRootArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final LogicalTypeRoot expectedElementRoot;
    private final @Nullable Boolean expectedArrayNullability;
    private final @Nullable Boolean expectedElementNullability;

    public ArrayOfRootArgumentTypeStrategy(
            LogicalTypeRoot expectedElementRoot,
            @Nullable Boolean expectedArrayNullability,
            @Nullable Boolean expectedElementNullability) {
        this.expectedElementRoot = expectedElementRoot;
        this.expectedArrayNullability = expectedArrayNullability;
        this.expectedElementNullability = expectedElementNullability;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        DataType actualDataType = callContext.getArgumentDataTypes().get(argumentPos);
        LogicalType actualLogicalType = actualDataType.getLogicalType();

        if (!actualLogicalType.is(LogicalTypeRoot.ARRAY)
                || Boolean.FALSE.equals(expectedArrayNullability)
                        && actualLogicalType.isNullable()) {
            return callContext.fail(
                    throwOnFailure,
                    "Unsupported argument type. Expected %stype of root 'ARRAY' but actual type was '%s'.",
                    Boolean.FALSE.equals(expectedArrayNullability) ? "NOT NULL " : "",
                    actualLogicalType);
        }

        return inferElementType(
                        callContext,
                        throwOnFailure,
                        ((CollectionDataType) actualDataType).getElementDataType())
                .map(DataTypes::ARRAY)
                .map(
                        dataType ->
                                Boolean.FALSE.equals(expectedArrayNullability)
                                                || expectedArrayNullability == null
                                                        && !actualLogicalType.isNullable()
                                        ? dataType.notNull()
                                        : dataType);
    }

    private Optional<DataType> inferElementType(
            CallContext callContext, boolean throwOnFailure, DataType elementDataType) {
        LogicalType elementLogicalType = elementDataType.getLogicalType();

        Optional<DataType> inferredElementDataType = Optional.empty();
        try {
            inferredElementDataType =
                    findDataType(
                            callContext,
                            throwOnFailure,
                            elementDataType,
                            expectedElementRoot,
                            expectedElementNullability);
        } catch (ValidationException t) {
            // wrap inner exception to provide more context about element
        }

        return inferredElementDataType.or(
                () ->
                        callContext.fail(
                                throwOnFailure,
                                "Unsupported argument type. Expected %selement type of root '%s' but actual type was '%s'.",
                                Boolean.FALSE.equals(expectedElementNullability) ? "NOT NULL " : "",
                                expectedElementRoot,
                                elementLogicalType));
    }

    @Override
    public Argument getExpectedArgument(FunctionDefinition def, int pos) {
        String elementType = expectedElementRoot.toString();
        if (Boolean.TRUE.equals(expectedElementNullability)) {
            elementType += " NULL";
        } else if (Boolean.FALSE.equals(expectedElementNullability)) {
            elementType += " NOT NULL";
        }
        String type = "ARRAY<" + elementType + ">";
        if (Boolean.TRUE.equals(expectedArrayNullability)) {
            type += " NULL";
        } else if (Boolean.FALSE.equals(expectedArrayNullability)) {
            type += " NOT NULL";
        }
        return Argument.of(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArrayOfRootArgumentTypeStrategy strategy = (ArrayOfRootArgumentTypeStrategy) o;
        return expectedElementRoot == strategy.expectedElementRoot
                && Objects.equals(expectedArrayNullability, strategy.expectedArrayNullability)
                && Objects.equals(expectedElementNullability, strategy.expectedElementNullability);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                expectedElementRoot, expectedArrayNullability, expectedElementNullability);
    }
}
