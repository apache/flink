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
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.LateralSnapshotTypeStrategy.INPUT_ARG_INDEX;
import static org.apache.flink.table.types.inference.strategies.SpecificTypeStrategies.LATERAL_SNAPSHOT_OUTPUT_TYPE_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LateralSnapshotTypeStrategy#OUTPUT_TYPE_STRATEGY}.
 *
 * <p>The output type of the {@code SNAPSHOT} table function is the row type of its {@code input}
 * table argument, with any time-attribute indicators materialized into regular timestamps.
 */
class LateralSnapshotOutputTypeStrategyTest extends TypeStrategiesTestBase {

    // Input row exercising forwarding of field names, assorted types and both nullabilities, a
    // regular TIMESTAMP column (must be left untouched) and a rowtime-attribute column.
    private static final DataType INPUT_TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("currency", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("rate", DataTypes.DECIMAL(10, 2)),
                    DataTypes.FIELD("valid_from", DataTypes.TIMESTAMP(3)),
                    DataTypes.FIELD(
                            "rowtime",
                            TypeConversions.fromLogicalToDataType(
                                    new TimestampType(true, TimestampKind.ROWTIME, 3))));

    private static final DataType OUTPUT_TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("currency", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("rate", DataTypes.DECIMAL(10, 2)),
                    DataTypes.FIELD("valid_from", DataTypes.TIMESTAMP(3)),
                    DataTypes.FIELD("rowtime", DataTypes.TIMESTAMP(3)));

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // ----------------------------------------------------------------------------
                // Output is derived from the table semantics, forwards every column unchanged
                // (except for time-attributes which are materialized) and ignores trailing scalar
                // arguments.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Input table type is forwarded, trailing scalar args do not affect output type",
                                LATERAL_SNAPSHOT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(INPUT_TABLE_TYPE, DataTypes.STRING(), DataTypes.TIMESTAMP(3))
                        .calledWithTableSemanticsAt(
                                INPUT_ARG_INDEX, new TableSemanticsMock(INPUT_TABLE_TYPE))
                        .expectDataType(OUTPUT_TABLE_TYPE),

                // ----------------------------------------------------------------------------
                // Invalid: 'input' argument is not a table.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Invalid: input is not a table",
                                LATERAL_SNAPSHOT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(DataTypes.STRING())
                        // Intentionally no table semantics registered at the input position.
                        .expectErrorMessage("Argument 'input' of SNAPSHOT must be a table."));
    }

    /**
     * Verifies that the output strips time-attribute indicators. Logical-type equality ignores the
     * time-attribute kind (see {@link TimestampType#equals}), so {@code expectDataType} in {@link
     * #testData()} cannot detect it; the kind is asserted explicitly here.
     */
    @Test
    void stripsTimeAttributeIndicators() {
        final CallContextMock callContext = new CallContextMock();
        callContext.argumentDataTypes = List.of(INPUT_TABLE_TYPE);
        callContext.tableSemantics =
                Map.of(INPUT_ARG_INDEX, new TableSemanticsMock(INPUT_TABLE_TYPE));

        // Sanity: the input must actually carry a time attribute, otherwise this test is vacuous.
        assertThat(LogicalTypeChecks.getFieldTypes(INPUT_TABLE_TYPE.getLogicalType()))
                .anyMatch(LogicalTypeChecks::isTimeAttribute);

        final DataType output =
                LATERAL_SNAPSHOT_OUTPUT_TYPE_STRATEGY.inferType(callContext).orElseThrow();

        assertThat(LogicalTypeChecks.getFieldTypes(output.getLogicalType()))
                .noneMatch(LogicalTypeChecks::isTimeAttribute);
    }
}
