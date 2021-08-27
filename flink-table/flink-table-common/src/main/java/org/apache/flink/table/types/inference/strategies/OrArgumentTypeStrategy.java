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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;

/**
 * Strategy for inferring and validating an argument using a disjunction of multiple {@link
 * ArgumentTypeStrategy}s into one like {@code f(NUMERIC || STRING)}.
 *
 * <p>Some {@link ArgumentTypeStrategy}s cannot contribute an inferred type that is different from
 * the input type (e.g. {@link InputTypeStrategies#LITERAL}). Therefore, the order {@code f(X || Y)}
 * or {@code f(Y || X)} matters as it defines the precedence in case the result must be casted to a
 * more specific type.
 *
 * <p>This strategy aims to infer a type that is equal to the input type (to prevent unnecessary
 * casting) or (if this is not possible) the first more specific, casted type.
 */
@Internal
public final class OrArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final List<? extends ArgumentTypeStrategy> argumentStrategies;

    public OrArgumentTypeStrategy(List<? extends ArgumentTypeStrategy> argumentStrategies) {
        Preconditions.checkArgument(argumentStrategies.size() > 0);
        this.argumentStrategies = argumentStrategies;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final LogicalType actualType =
                callContext.getArgumentDataTypes().get(argumentPos).getLogicalType();

        Optional<DataType> closestDataType = Optional.empty();
        for (ArgumentTypeStrategy strategy : argumentStrategies) {
            final Optional<DataType> inferredDataType =
                    strategy.inferArgumentType(callContext, argumentPos, false);
            // argument type does not match at all
            if (!inferredDataType.isPresent()) {
                continue;
            }
            final LogicalType inferredType = inferredDataType.get().getLogicalType();
            // argument type matches
            // we prefer a strategy that does not require an implicit cast
            if (supportsAvoidingCast(actualType, inferredType)) {
                return inferredDataType;
            }
            // argument type requires a more specific, casted type
            else if (!closestDataType.isPresent()) {
                closestDataType = inferredDataType;
            }
        }

        if (closestDataType.isPresent()) {
            return closestDataType;
        }

        // generate a helpful exception if possible
        if (throwOnFailure) {
            for (ArgumentTypeStrategy strategy : argumentStrategies) {
                strategy.inferArgumentType(callContext, argumentPos, true);
            }
        }

        return Optional.empty();
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        final String argument =
                argumentStrategies.stream()
                        .map(v -> v.getExpectedArgument(functionDefinition, argumentPos).getType())
                        .collect(Collectors.joining(" | ", "[", "]"));
        return Signature.Argument.of(argument);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrArgumentTypeStrategy that = (OrArgumentTypeStrategy) o;
        return Objects.equals(argumentStrategies, that.argumentStrategies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentStrategies);
    }
}
