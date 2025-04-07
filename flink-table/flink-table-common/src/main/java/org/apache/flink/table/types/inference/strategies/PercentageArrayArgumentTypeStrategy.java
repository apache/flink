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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Optional;

/**
 * An {@link ArgumentTypeStrategy} that expects an array of percentages with each element between
 * [0.0, 1.0].
 */
@Internal
public final class PercentageArrayArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final boolean expectedNullability;

    public PercentageArrayArgumentTypeStrategy(boolean nullable) {
        this.expectedNullability = nullable;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final LogicalType actualType =
                callContext.getArgumentDataTypes().get(argumentPos).getLogicalType();

        if (!actualType.is(LogicalTypeRoot.ARRAY)) {
            return callContext.fail(throwOnFailure, "Percentage must be an array.");
        }
        if (!expectedNullability && actualType.isNullable()) {
            return callContext.fail(throwOnFailure, "Percentage must be a non-null array.");
        }

        LogicalType elementType = ((ArrayType) actualType).getElementType();
        if (!elementType.is(LogicalTypeFamily.NUMERIC)) {
            return callContext.fail(
                    throwOnFailure, "Value in the percentage array must be of NUMERIC type.");
        }
        if (!expectedNullability && elementType.isNullable()) {
            return callContext.fail(
                    throwOnFailure, "Value in the percentage array must be of NOT NULL type.");
        }

        if (callContext.isArgumentLiteral(argumentPos)) {
            Optional<Number[]> literalVal =
                    callContext.getArgumentValue(argumentPos, Number[].class);

            if (!literalVal.isPresent()) {
                return callContext.fail(
                        throwOnFailure,
                        "Percentage must be an array of NUMERIC values between [0.0, 1.0].");
            }

            for (Number value : literalVal.get()) {
                if (value.doubleValue() < 0.0 || value.doubleValue() > 1.0) {
                    return callContext.fail(
                            throwOnFailure,
                            "Value in the percentage array must be between [0.0, 1.0], but was '%s'.",
                            value.doubleValue());
                }
            }
        }

        return Optional.of(
                expectedNullability
                        ? DataTypes.ARRAY(DataTypes.DOUBLE())
                        : DataTypes.ARRAY(DataTypes.DOUBLE().notNull()).notNull());
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return Signature.Argument.of(
                expectedNullability ? "ARRAY<NUMERIC>" : "ARRAY<NUMERIC NOT NULL> NOT NULL");
    }
}
