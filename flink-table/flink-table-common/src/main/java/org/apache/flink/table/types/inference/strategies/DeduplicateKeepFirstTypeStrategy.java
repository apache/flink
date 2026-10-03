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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DeduplicateKeepFirstTypeStrategy {

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

    private static final String SEEN_STATE_NAME = "seen";

    public static final StateTypeStrategy SEEN_STATE_TYPE_STRATEGY =
            new StateTypeStrategy() {
                @Override
                public Optional<DataType> inferType(final CallContext callContext) {
                    return Optional.of(
                            ValueView.newValueViewDataType(DataTypes.BOOLEAN().notNull()));
                }

                @Override
                public Optional<Duration> getTimeToLive(final CallContext callContext) {
                    return callContext.getArgumentValue(ARG_STATE_TTL, Duration.class);
                }
            };

    private static final String CANDIDATE_STATE_NAME = "candidate";

    public static final StateTypeStrategy CANDIDATE_STATE_TYPE_STRATEGY =
            new StateTypeStrategy() {
                @Override
                public Optional<DataType> inferType(final CallContext callContext) {
                    final TableSemantics tableSemantics =
                            callContext
                                    .getTableSemantics(ARG_INPUT)
                                    .orElseThrow(
                                            () ->
                                                    new ValidationException(
                                                            "First argument must be a table for DEDUPLICATE_KEEP_FIRST."));
                    final List<Field> inputFields = DataType.getFields(tableSemantics.dataType());
                    final List<Field> candidateFields =
                            Arrays.stream(
                                            ChangelogTypeStrategyUtils.computeOutputIndices(
                                                    tableSemantics))
                                    .mapToObj(inputFields::get)
                                    .collect(Collectors.toCollection(ArrayList::new));
                    // The trailing timestamp field is read positionally at runtime, so its name
                    // only needs to avoid clashing with a payload column of the same name.
                    final Set<String> takenNames =
                            candidateFields.stream()
                                    .map(Field::getName)
                                    .collect(Collectors.toSet());
                    String timestampField = "event_time";
                    for (int i = 0; takenNames.contains(timestampField); i++) {
                        timestampField = "event_time_" + i;
                    }
                    candidateFields.add(DataTypes.FIELD(timestampField, DataTypes.BIGINT()));
                    return Optional.of(
                            ValueView.newValueViewDataType(DataTypes.ROW(candidateFields)));
                }

                @Override
                public Optional<Duration> getTimeToLive(final CallContext callContext) {
                    // The candidate buffer is cleared by its timer, not by retention; disable its
                    // TTL so a global table.exec.state.ttl cannot expire it before the timer fires.
                    return Optional.of(Duration.ZERO);
                }
            };

    public static LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies() {
        final LinkedHashMap<String, StateTypeStrategy> strategies = new LinkedHashMap<>();
        strategies.put(SEEN_STATE_NAME, SEEN_STATE_TYPE_STRATEGY);
        strategies.put(CANDIDATE_STATE_NAME, CANDIDATE_STATE_TYPE_STRATEGY);
        return strategies;
    }

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
                            + "Use INTERVAL '0' to disable state retention.",
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
}
