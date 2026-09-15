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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.ColumnList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Type strategies for the {@code SNAPSHOT} table function used by the {@code LATERAL SNAPSHOT}
 * temporal join.
 *
 * <p>Validates the named arguments
 *
 * <ul>
 *   <li>{@code input} (TABLE, required)
 *   <li>{@code on_time} (DESCRIPTOR, optional; required for streaming, enforced by the rule)
 *   <li>{@code load_completed_time} (TIMESTAMP_LTZ(3), optional; when absent, the planner rule
 *       defaults the load completed time to the wall-clock time when the query is compiled)
 *   <li>{@code load_completed_idle_timeout} (INTERVAL SECOND, optional)
 *   <li>{@code state_ttl} (INTERVAL SECOND, optional)
 * </ul>
 *
 * <p>The output type forwards the input table's row type, but materializes any rowtime attribute
 * indicator into a regular timestamp.
 */
@Internal
public final class LateralSnapshotTypeStrategy {

    /** The {@code input} TABLE argument. */
    public static final int INPUT_ARG_INDEX = 0;

    public static final String INPUT_ARG_NAME = "input";

    /** The {@code on_time} DESCRIPTOR argument naming the build-side row-time column. */
    public static final int ON_TIME_ARG_INDEX = 1;

    public static final String ON_TIME_ARG_NAME = "on_time";

    /** The {@code load_completed_time} TIMESTAMP_LTZ argument. */
    public static final int LOAD_COMPLETED_TIME_ARG_INDEX = 2;

    public static final String LOAD_COMPLETED_TIME_ARG_NAME = "load_completed_time";

    /** The {@code load_completed_idle_timeout} INTERVAL argument. */
    public static final int LOAD_COMPLETED_IDLE_TIMEOUT_ARG_INDEX = 3;

    public static final String LOAD_COMPLETED_IDLE_TIMEOUT_ARG_NAME = "load_completed_idle_timeout";

    /** The {@code state_ttl} INTERVAL argument. */
    public static final int STATE_TTL_ARG_INDEX = 4;

    public static final String STATE_TTL_ARG_NAME = "state_ttl";

    // --------------------------------------------------------------------------------------------
    // Input validation
    // --------------------------------------------------------------------------------------------

    public static final InputTypeStrategy INPUT_TYPE_STRATEGY =
            new InputTypeStrategy() {
                @Override
                public ArgumentCount getArgumentCount() {
                    return ConstantArgumentCount.between(1, 5);
                }

                @Override
                public Optional<List<DataType>> inferInputTypes(
                        final CallContext callContext, final boolean throwOnFailure) {
                    return validateInputs(callContext, throwOnFailure);
                }

                @Override
                public List<Signature> getExpectedSignatures(final FunctionDefinition definition) {
                    return List.of(
                            Signature.of(
                                    Argument.of(INPUT_ARG_NAME, "TABLE"),
                                    Argument.of(ON_TIME_ARG_NAME, "DESCRIPTOR"),
                                    Argument.of(LOAD_COMPLETED_TIME_ARG_NAME, "TIMESTAMP_LTZ(3)"),
                                    Argument.of(
                                            LOAD_COMPLETED_IDLE_TIMEOUT_ARG_NAME,
                                            "INTERVAL SECOND"),
                                    Argument.of(STATE_TTL_ARG_NAME, "INTERVAL SECOND")));
                }
            };

    // --------------------------------------------------------------------------------------------
    // Output type inference: forward the input table row type with time attributes materialized.
    // --------------------------------------------------------------------------------------------

    public static final TypeStrategy OUTPUT_TYPE_STRATEGY =
            callContext -> {
                final TableSemantics semantics =
                        callContext
                                .getTableSemantics(INPUT_ARG_INDEX)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "Argument 'input' of SNAPSHOT must be a table."));
                return Optional.of(materializeTimeAttributes(semantics.dataType()));
            };

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /**
     * Rebuilds {@code inputTableType} as a ROW with all fields identical to the input, except for
     * time attributes which get stripped off their time indicator property.
     */
    private static DataType materializeTimeAttributes(final DataType inputTableType) {
        final List<DataType> fieldTypes = DataType.getFieldDataTypes(inputTableType);
        final List<String> fieldNames = DataType.getFieldNames(inputTableType);
        final List<Field> fields =
                IntStream.range(0, fieldTypes.size())
                        .mapToObj(
                                pos ->
                                        DataTypes.FIELD(
                                                fieldNames.get(pos),
                                                DataTypeUtils.removeTimeAttribute(
                                                        fieldTypes.get(pos))))
                        .collect(Collectors.toList());
        return DataTypes.ROW(fields);
    }

    private static Optional<List<DataType>> validateInputs(
            final CallContext callContext, final boolean throwOnFailure) {
        if (callContext.getTableSemantics(INPUT_ARG_INDEX).isEmpty()) {
            return callContext.fail(
                    throwOnFailure, "Argument 'input' of SNAPSHOT must be a table.");
        }

        // Validate on_time if provided. Presence is enforced by the planner rule for streaming.
        final Optional<List<DataType>> timeColumnFailure =
                validateOnTime(callContext, throwOnFailure);
        if (timeColumnFailure != null) {
            return timeColumnFailure;
        }

        return Optional.of(callContext.getArgumentDataTypes());
    }

    /**
     * Validates {@code on_time} when present: it must name exactly one existing TIMESTAMP or
     * TIMESTAMP_LTZ column (precision up to 3). Returns {@code null} when the argument is absent or
     * valid; otherwise returns the failure result of {@link CallContext#fail}.
     */
    private static Optional<List<DataType>> validateOnTime(
            final CallContext callContext, final boolean throwOnFailure) {
        if (!isArgumentProvided(callContext, ON_TIME_ARG_INDEX)) {
            return null;
        }
        final List<String> columns =
                callContext
                        .getArgumentValue(ON_TIME_ARG_INDEX, ColumnList.class)
                        .map(ColumnList::getNames)
                        .orElse(List.of());
        if (columns.size() != 1) {
            return callContext.fail(
                    throwOnFailure,
                    "Argument 'on_time' of SNAPSHOT must reference exactly one column.");
        }
        final String timeColumn = columns.get(0);
        final DataType inputType = callContext.getArgumentDataTypes().get(INPUT_ARG_INDEX);
        final int idx = DataType.getFieldNames(inputType).indexOf(timeColumn);
        if (idx < 0) {
            return callContext.fail(
                    throwOnFailure,
                    "Argument 'on_time' of SNAPSHOT references column '%s' which is not present "
                            + "in the input table.",
                    timeColumn);
        }
        final LogicalType columnType =
                DataType.getFieldDataTypes(inputType).get(idx).getLogicalType();
        if (SystemTypeInference.isUnsupportedOnTimeColumn(columnType)) {
            return callContext.fail(
                    throwOnFailure,
                    "Argument 'on_time' of SNAPSHOT must reference a TIMESTAMP or TIMESTAMP_LTZ "
                            + "column (up to precision 3), but column '%s' has type '%s'.",
                    timeColumn,
                    columnType.asSummaryString());
        }
        return null;
    }

    private static boolean isArgumentProvided(final CallContext callContext, final int index) {
        return callContext.getArgumentDataTypes().size() > index
                && !callContext.isArgumentNull(index);
    }

    private LateralSnapshotTypeStrategy() {}
}
