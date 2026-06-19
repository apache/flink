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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.Optional;

/** An {@link ArgumentTypeStrategy} that expects a percentage value between [0.0, 1.0]. */
@Internal
public final class PercentageArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final boolean expectedNullability;

    public PercentageArgumentTypeStrategy(boolean nullable) {
        this.expectedNullability = nullable;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final LogicalType actualType =
                callContext.getArgumentDataTypes().get(argumentPos).getLogicalType();

        if (!actualType.is(LogicalTypeFamily.NUMERIC)) {
            return callContext.fail(throwOnFailure, "Percentage must be of NUMERIC type.");
        }
        if (!expectedNullability && actualType.isNullable()) {
            return callContext.fail(throwOnFailure, "Percentage must be of NOT NULL type.");
        }

        if (callContext.isArgumentLiteral(argumentPos)) {
            Optional<Number> literalVal = callContext.getArgumentValue(argumentPos, Number.class);

            Double val = null;
            if (literalVal.isPresent()) {
                val = literalVal.get().doubleValue();
            }

            if (val == null || val < 0.0 || val > 1.0) {
                return callContext.fail(
                        throwOnFailure,
                        "Percentage must be between [0.0, 1.0], but was '%s'.",
                        val);
            }
        }

        return Optional.of(expectedNullability ? DataTypes.DOUBLE() : DataTypes.DOUBLE().notNull());
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return Signature.Argument.of(expectedNullability ? "<NUMERIC>" : "<NUMERIC NOT NULL>");
    }
}
