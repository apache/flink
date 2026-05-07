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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY;

/**
 * Tests for {@link SpecificInputTypeStrategies#LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY}.
 *
 * <p>Validates the named-argument signature of the {@code SNAPSHOT} table function, including the
 * cross-argument consistency between {@code load_completed_condition} and {@code
 * load_completed_time}.
 */
class LateralSnapshotInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final DataType TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.STRING()),
                    DataTypes.FIELD("v", DataTypes.INT()));

    private static final DataType STRING_TYPE = DataTypes.STRING();
    private static final DataType TIMESTAMP_TYPE = DataTypes.TIMESTAMP(3);
    private static final DataType INTERVAL_TYPE = DataTypes.INTERVAL(DataTypes.SECOND());

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // ----------------------------------------------------------------------------
                // Valid: just the build-side table.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Valid: input only (default condition)",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .expectArgumentTypes(TABLE_TYPE),

                // ----------------------------------------------------------------------------
                // Valid: explicit 'compile_time' condition without load_completed_time.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Valid: condition='compile_time'",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, STRING_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "compile_time")
                        .expectArgumentTypes(TABLE_TYPE, STRING_TYPE),

                // ----------------------------------------------------------------------------
                // Valid: 'user_time' with a TIMESTAMP literal.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Valid: condition='user_time' + load_completed_time",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, STRING_TYPE, TIMESTAMP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "user_time")
                        .calledWithLiteralAt(2, LocalDateTime.parse("2026-07-01T00:00:00.001"))
                        .expectArgumentTypes(TABLE_TYPE, STRING_TYPE, TIMESTAMP_TYPE),

                // ----------------------------------------------------------------------------
                // Valid: full named-arg form with idle timeout and TTL.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy("Valid: full args", LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE,
                                STRING_TYPE,
                                TIMESTAMP_TYPE,
                                INTERVAL_TYPE,
                                INTERVAL_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "user_time")
                        .calledWithLiteralAt(2, LocalDateTime.parse("2026-07-01T00:00:00.001"))
                        .calledWithLiteralAt(3, Duration.ofSeconds(10))
                        .calledWithLiteralAt(4, Duration.ofDays(1))
                        .expectArgumentTypes(
                                TABLE_TYPE,
                                STRING_TYPE,
                                TIMESTAMP_TYPE,
                                INTERVAL_TYPE,
                                INTERVAL_TYPE),

                // ----------------------------------------------------------------------------
                // Invalid: No arguments
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy("Invalid: no arguments", LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes()
                        // Intentionally no arguments.
                        .expectErrorMessage("Invalid function call"),

                // ----------------------------------------------------------------------------
                // Invalid: 'input' argument is not a table.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: input is not a table",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(STRING_TYPE)
                        // Intentionally no table type registered at position 0.
                        .expectErrorMessage("Argument 'input' of SNAPSHOT must be a table."),

                // ----------------------------------------------------------------------------
                // Invalid: 'user_time' condition requires load_completed_time.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: condition='user_time' without load_completed_time",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, STRING_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "user_time")
                        .expectErrorMessage(
                                "SNAPSHOT requires 'load_completed_time' when "
                                        + "'load_completed_condition' is 'user_time'."),

                // ----------------------------------------------------------------------------
                // Invalid: load_completed_time requires 'user_time' condition.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: load_completed_time without explicit 'user_time'",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, STRING_TYPE, TIMESTAMP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "compile_time")
                        .calledWithLiteralAt(2, LocalDateTime.parse("2026-07-01T00:00:00.001"))
                        .expectErrorMessage(
                                "SNAPSHOT does not accept 'load_completed_time' when "
                                        + "'load_completed_condition' is not 'user_time'."),

                // ----------------------------------------------------------------------------
                // Invalid: unknown condition value.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: unknown condition value",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, STRING_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "invalid_condition")
                        .expectErrorMessage(
                                "Argument 'load_completed_condition' of SNAPSHOT must be one of 'compile_time', 'user_time' but was 'invalid_condition'."),

                // ----------------------------------------------------------------------------
                // Invalid: load_completed_condition provided as a non-literal expression.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: non-literal load_completed_condition",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, STRING_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        // Intentionally no literal provided for load_completed_condition
                        .expectErrorMessage(
                                "Argument 'load_completed_condition' of SNAPSHOT must be a STRING literal."),

                // ----------------------------------------------------------------------------
                // Invalid: load_completed_time provided as a non-literal expression.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: non-literal load_completed_time",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, STRING_TYPE, TIMESTAMP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "user_time")
                        // Intentionally no literal provided for load_completed_time
                        .expectErrorMessage(
                                "Argument 'load_completed_time' of SNAPSHOT must be a TIMESTAMP literal."),

                // ----------------------------------------------------------------------------
                // Invalid: load_completed_idle_timeout provided as a non-literal expression.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: non-literal load_completed_idle_timeout",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, STRING_TYPE, TIMESTAMP_TYPE, INTERVAL_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "user_time")
                        .calledWithLiteralAt(2, LocalDateTime.parse("2026-07-01T00:00:00.001"))
                        // Intentionally no literal provided for load_completed_idle_timeout
                        .expectErrorMessage(
                                "Argument 'load_completed_idle_timeout' of SNAPSHOT must be an INTERVAL literal."),

                // ----------------------------------------------------------------------------
                // Invalid: state_ttl provided as a non-literal expression.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: non-literal state_ttl",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE,
                                STRING_TYPE,
                                TIMESTAMP_TYPE,
                                INTERVAL_TYPE,
                                INTERVAL_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, "user_time")
                        .calledWithLiteralAt(2, LocalDateTime.parse("2026-07-01T00:00:00.001"))
                        .calledWithLiteralAt(3, Duration.ofSeconds(10))
                        // Intentionally no literal provided for state_ttl
                        .expectErrorMessage(
                                "Argument 'state_ttl' of SNAPSHOT must be an INTERVAL literal."));
    }
}
