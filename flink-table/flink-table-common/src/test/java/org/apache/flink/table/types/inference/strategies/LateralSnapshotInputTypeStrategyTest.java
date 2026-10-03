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
import org.apache.flink.types.ColumnList;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY;

/**
 * Tests for {@link SpecificInputTypeStrategies#LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY}.
 *
 * <p>Validates the named-argument signature of the {@code SNAPSHOT} table function.
 */
class LateralSnapshotInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final DataType TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.STRING()),
                    DataTypes.FIELD("v", DataTypes.INT()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)));

    private static final DataType STRING_TYPE = DataTypes.STRING();
    private static final DataType DESCRIPTOR_TYPE = DataTypes.DESCRIPTOR();
    private static final DataType TIMESTAMP_TYPE = DataTypes.TIMESTAMP(3);
    private static final DataType INTERVAL_TYPE = DataTypes.INTERVAL(DataTypes.SECOND());

    private static final ColumnList ON_TIME = ColumnList.of("ts");

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // ----------------------------------------------------------------------------
                // Valid: just the build-side table (on_time is optional at this layer; the
                // planner rule enforces it for streaming).
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy("Valid: input only", LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .expectArgumentTypes(TABLE_TYPE),

                // ----------------------------------------------------------------------------
                // Valid: input + on_time.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy("Valid: input + on_time", LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ON_TIME)
                        .expectArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE),

                // ----------------------------------------------------------------------------
                // Valid: full named-arg form with idle timeout and TTL.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy("Valid: full args", LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE,
                                DESCRIPTOR_TYPE,
                                TIMESTAMP_TYPE,
                                INTERVAL_TYPE,
                                INTERVAL_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ON_TIME)
                        .calledWithLiteralAt(2, LocalDateTime.parse("2026-07-01T00:00:00.001"))
                        .calledWithLiteralAt(3, Duration.ofSeconds(10))
                        .calledWithLiteralAt(4, Duration.ofDays(1))
                        .expectArgumentTypes(
                                TABLE_TYPE,
                                DESCRIPTOR_TYPE,
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
                // Invalid: on_time references an unknown column.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: on_time references unknown column",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of("nonexistent"))
                        .expectErrorMessage(
                                "Argument 'on_time' of SNAPSHOT references column 'nonexistent' "
                                        + "which is not present in the input table."),

                // ----------------------------------------------------------------------------
                // Invalid: on_time references a non-timestamp column.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: on_time references non-timestamp column",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of("v"))
                        .expectErrorMessage(
                                "Argument 'on_time' of SNAPSHOT must reference a TIMESTAMP or "
                                        + "TIMESTAMP_LTZ column (up to precision 3), but column 'v' "
                                        + "has type 'INT'."),

                // ----------------------------------------------------------------------------
                // Invalid: on_time references no column.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: on_time references no column",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of())
                        .expectErrorMessage(
                                "Argument 'on_time' of SNAPSHOT must reference exactly one column."),

                // ----------------------------------------------------------------------------
                // Invalid: on_time references more than one column.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: on_time references multiple columns",
                                LATERAL_SNAPSHOT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of("ts", "k"))
                        .expectErrorMessage(
                                "Argument 'on_time' of SNAPSHOT must reference exactly one column."));
    }
}
