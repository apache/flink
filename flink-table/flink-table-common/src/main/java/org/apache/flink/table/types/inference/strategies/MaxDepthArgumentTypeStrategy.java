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
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Optional;

/**
 * An {@link ArgumentTypeStrategy} that expects a positive integer literal for the {@code maxDepth}
 * parameter of {@code URL_DECODE_RECURSIVE}.
 *
 * <p>Accepted SQL integer types and their corresponding Java literal classes:
 *
 * <ul>
 *   <li>{@code TINYINT} {@link Byte}
 *   <li>{@code SMALLINT} {@link Short}
 *   <li>{@code INT} {@link Integer}
 *   <li>{@code BIGINT} {@link Long}
 * </ul>
 *
 * <p>The argument must be a compile-time literal (not a dynamic/runtime value), and its value must
 * be strictly positive ({@code > 0}).
 */
@Internal
public final class MaxDepthArgumentTypeStrategy implements ArgumentTypeStrategy {

    private final boolean expectedNullability;

    public MaxDepthArgumentTypeStrategy(boolean nullable) {
        this.expectedNullability = nullable;
    }

    @Override
    public Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure) {
        final var actualType = callContext.getArgumentDataTypes().get(argumentPos).getLogicalType();

        if (!actualType.is(LogicalTypeFamily.INTEGER_NUMERIC)) {
            return callContext.fail(throwOnFailure, "maxDepth must be of INTEGER type.");
        }
        if (!expectedNullability && actualType.isNullable()) {
            return callContext.fail(throwOnFailure, "maxDepth must be of NOT NULL type.");
        }

        // maxDepth must be a compile-time literal; dynamic (runtime) arguments are not accepted.
        if (!callContext.isArgumentLiteral(argumentPos)) {
            return callContext.fail(
                    throwOnFailure,
                    "maxDepth must be a literal integer, but was a dynamic argument.");
        }

        // Retrieve the literal value regardless of the concrete integer subtype
        // (TINYINT->Byte, SMALLINT->Short, INT->Integer, BIGINT->Long).
        final Optional<Long> maxDepthOpt =
                getLiteralAsLong(callContext, argumentPos, actualType.getTypeRoot());
        if (maxDepthOpt.isPresent()) {
            long maxDepth = maxDepthOpt.get();
            if (maxDepth <= 0) {
                return callContext.fail(
                        throwOnFailure,
                        "maxDepth must be a positive integer, but was: " + maxDepth);
            }
        }

        // Preserve the actual integer type so that the runtime eval() dispatches to the
        // correct overload (Byte / Short / Integer / Long).
        return Optional.of(resolveOutputType(actualType.getTypeRoot(), expectedNullability));
    }

    @Override
    public Signature.Argument getExpectedArgument(
            FunctionDefinition functionDefinition, int argumentPos) {
        return Signature.Argument.of(expectedNullability ? "<INTEGER>" : "<INTEGER NOT NULL>");
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the output {@link DataType} that corresponds to the given integer {@link
     * LogicalTypeRoot}, with the requested nullability.
     */
    private static DataType resolveOutputType(LogicalTypeRoot root, boolean nullable) {
        final DataType base;
        switch (root) {
            case TINYINT:
                base = DataTypes.TINYINT();
                break;
            case SMALLINT:
                base = DataTypes.SMALLINT();
                break;
            case BIGINT:
                base = DataTypes.BIGINT();
                break;
            case INTEGER:
            default:
                base = DataTypes.INT();
                break;
        }
        return nullable ? base : base.notNull();
    }

    /**
     * Retrieves the literal integer value as a {@link Long}, regardless of the actual integer
     * subtype ({@code TINYINT}/{@code SMALLINT}/{@code INT}/{@code BIGINT}).
     */
    private static Optional<Long> getLiteralAsLong(
            CallContext callContext, int argumentPos, LogicalTypeRoot root) {
        switch (root) {
            case TINYINT:
                return callContext.getArgumentValue(argumentPos, Byte.class).map(Byte::longValue);
            case SMALLINT:
                return callContext.getArgumentValue(argumentPos, Short.class).map(Short::longValue);
            case BIGINT:
                return callContext.getArgumentValue(argumentPos, Long.class);
            case INTEGER:
            default:
                return callContext
                        .getArgumentValue(argumentPos, Integer.class)
                        .map(Integer::longValue);
        }
    }
}
