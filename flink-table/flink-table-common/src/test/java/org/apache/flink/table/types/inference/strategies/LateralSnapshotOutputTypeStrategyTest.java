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
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;

import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.LateralSnapshotTypeStrategy.INPUT_ARG_INDEX;
import static org.apache.flink.table.types.inference.strategies.SpecificTypeStrategies.LATERAL_SNAPSHOT_OUTPUT_TYPE_STRATEGY;

/**
 * Tests for {@link LateralSnapshotTypeStrategy#OUTPUT_TYPE_STRATEGY}.
 *
 * <p>The output type of the {@code SNAPSHOT} table function is the row type of its {@code input}
 * table argument, passed through unchanged.
 */
class LateralSnapshotOutputTypeStrategyTest extends TypeStrategiesTestBase {

    private static final DataType TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.STRING()),
                    DataTypes.FIELD("v", DataTypes.INT()));

    private static final DataType OTHER_TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("currency", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("rate", DataTypes.DECIMAL(10, 2)),
                    DataTypes.FIELD("valid_from", DataTypes.TIMESTAMP(3)));

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // ----------------------------------------------------------------------------
                // Pass-through: output equals the input table's row type.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Output is the input table row type",
                                LATERAL_SNAPSHOT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(TABLE_TYPE)
                        .calledWithTableSemanticsAt(
                                INPUT_ARG_INDEX, new TableSemanticsMock(TABLE_TYPE))
                        .expectDataType(TABLE_TYPE),

                // ----------------------------------------------------------------------------
                // Pass-through preserves field names, nested types and nullability.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Output preserves field names and nullability",
                                LATERAL_SNAPSHOT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(OTHER_TABLE_TYPE)
                        .calledWithTableSemanticsAt(
                                INPUT_ARG_INDEX, new TableSemanticsMock(OTHER_TABLE_TYPE))
                        .expectDataType(OTHER_TABLE_TYPE),

                // ----------------------------------------------------------------------------
                // Pass-through uses the table semantics' type, ignoring extra scalar arguments.
                // ----------------------------------------------------------------------------
                TestSpec.forStrategy(
                                "Output ignores trailing scalar arguments",
                                LATERAL_SNAPSHOT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(TABLE_TYPE, DataTypes.STRING(), DataTypes.TIMESTAMP(3))
                        .calledWithTableSemanticsAt(
                                INPUT_ARG_INDEX, new TableSemanticsMock(TABLE_TYPE))
                        .expectDataType(TABLE_TYPE),

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
}
