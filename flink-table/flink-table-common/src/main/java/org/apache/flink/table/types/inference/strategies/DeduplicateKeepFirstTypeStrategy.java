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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Type strategies for the {@code DEDUPLICATE_KEEP_FIRST} process table function. */
@Internal
public final class DeduplicateKeepFirstTypeStrategy {

    public static final int ARG_INPUT = 0;
    public static final int ARG_STATE_TTL = 1;
    public static final int ARG_RESET_TTL_ON_DUPLICATE = 2;

    public static final InputTypeStrategy INPUT_TYPE_STRATEGY =
            new ValidationOnlyInputTypeStrategy() {
                @Override
                public Optional<List<DataType>> inferInputTypes(
                        final CallContext callContext, final boolean throwOnFailure) {
                    final Optional<List<DataType>> stateTtlError =
                            validateStateTtl(callContext, throwOnFailure);
                    if (stateTtlError.isPresent()) {
                        return stateTtlError;
                    }

                    final Optional<List<DataType>> resetTtlError =
                            validateResetTtlOnDuplicate(callContext, throwOnFailure);
                    if (resetTtlError.isPresent()) {
                        return resetTtlError;
                    }

                    return Optional.of(callContext.getArgumentDataTypes());
                }
            };

    public static final TypeStrategy OUTPUT_TYPE_STRATEGY =
            callContext -> {
                final TableSemantics tableSemantics =
                        callContext
                                .getTableSemantics(ARG_INPUT)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "First argument must be a table for DEDUPLICATE_KEEP_FIRST."));

                final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
                final List<Field> outputFields =
                        Arrays.stream(
                                        ChangelogTypeStrategyUtils.computeOutputIndices(
                                                tableSemantics))
                                .mapToObj(inputFields::get)
                                .collect(Collectors.toList());
                return Optional.of(DataTypes.ROW(outputFields).notNull());
            };

    private static Optional<List<DataType>> validateStateTtl(
            final CallContext callContext, final boolean throwOnFailure) {
        if (callContext.isArgumentNull(ARG_STATE_TTL)) {
            return Optional.empty();
        }
        if (!callContext.isArgumentLiteral(ARG_STATE_TTL)) {
            return callContext.fail(
                    throwOnFailure,
                    "The 'state_ttl' argument must be a constant INTERVAL literal.");
        }
        final Optional<Duration> stateTtl =
                callContext.getArgumentValue(ARG_STATE_TTL, Duration.class);
        if (stateTtl.isPresent() && stateTtl.get().isNegative()) {
            return callContext.fail(
                    throwOnFailure,
                    "The 'state_ttl' argument must not be negative, but was '%s'. "
                            + "Use a positive INTERVAL to bound state retention, INTERVAL '0' to "
                            + "retain state indefinitely without a TTL, or omit the argument to "
                            + "fall back to session defaults.",
                    stateTtl.get());
        }
        return Optional.empty();
    }

    private static Optional<List<DataType>> validateResetTtlOnDuplicate(
            final CallContext callContext, final boolean throwOnFailure) {
        if (callContext.isArgumentNull(ARG_RESET_TTL_ON_DUPLICATE)) {
            return Optional.empty();
        }
        if (!callContext.isArgumentLiteral(ARG_RESET_TTL_ON_DUPLICATE)) {
            return callContext.fail(
                    throwOnFailure,
                    "The 'reset_ttl_on_duplicate' argument must be a constant BOOLEAN literal.");
        }
        return Optional.empty();
    }

    private DeduplicateKeepFirstTypeStrategy() {}
}
