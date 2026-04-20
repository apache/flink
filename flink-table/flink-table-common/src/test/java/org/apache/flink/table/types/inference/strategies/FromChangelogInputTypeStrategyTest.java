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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.FROM_CHANGELOG_INPUT_TYPE_STRATEGY;

/** Tests for {@link FromChangelogTypeStrategy#INPUT_TYPE_STRATEGY}. */
class FromChangelogInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final DataType TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT()),
                    DataTypes.FIELD("op", DataTypes.STRING()),
                    DataTypes.FIELD("name", DataTypes.STRING()));

    private static final DataType DESCRIPTOR_TYPE = DataTypes.DESCRIPTOR();

    private static final DataType MAP_TYPE = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // Valid: custom mapping with all change operations
                TestSpec.forStrategy(
                                "Valid with custom mapping", FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("op")))
                        .calledWithLiteralAt(
                                2,
                                Map.of(
                                        "c", "INSERT",
                                        "ub", "UPDATE_BEFORE",
                                        "ua", "UPDATE_AFTER",
                                        "d", "DELETE"))
                        .expectArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE),

                // Error: op column not found
                TestSpec.forStrategy(
                                "Op column not found in schema", FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("nonexistent")))
                        .expectErrorMessage("The op column 'nonexistent' does not exist"),

                // Error: op column is not STRING
                TestSpec.forStrategy("Op column wrong type", FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("op", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING())),
                                DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(
                                0,
                                new TableSemanticsMock(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("id", DataTypes.INT()),
                                                DataTypes.FIELD("op", DataTypes.INT()),
                                                DataTypes.FIELD("name", DataTypes.STRING()))))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("op")))
                        .expectErrorMessage("must be of STRING type"),

                // Error: multi-column descriptor
                TestSpec.forStrategy(
                                "Descriptor with multiple columns",
                                FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("a", "b")))
                        .expectErrorMessage("must contain exactly one column name"),

                // Error: invalid RowKind in op_mapping value
                TestSpec.forStrategy(
                                "Invalid RowKind in mapping value",
                                FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("op")))
                        .calledWithLiteralAt(2, Map.of("c", "INVALID_KIND"))
                        .expectErrorMessage("Unknown change operation: 'INVALID_KIND'"),

                // Error: duplicate RowKind across entries
                TestSpec.forStrategy(
                                "Duplicate RowKind in mapping values",
                                FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("op")))
                        .calledWithLiteralAt(2, Map.of("c", "INSERT", "r", "INSERT"))
                        .expectErrorMessage("Duplicate change operation: 'INSERT'"),

                // Valid: INSERT-only mapping (append mode, no updates)
                TestSpec.forStrategy(
                                "Valid INSERT-only mapping", FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("op")))
                        .calledWithLiteralAt(2, Map.of("c, r", "INSERT"))
                        .expectArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE),

                // Valid: INSERT + DELETE mapping (no updates)
                TestSpec.forStrategy(
                                "Valid INSERT and DELETE mapping",
                                FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("op")))
                        .calledWithLiteralAt(2, Map.of("c", "INSERT", "d", "DELETE"))
                        .expectArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE),

                // Error: UPDATE_AFTER without UPDATE_BEFORE not supported
                TestSpec.forStrategy(
                                "UPDATE_AFTER requires UPDATE_BEFORE",
                                FROM_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(1, ColumnList.of(List.of("op")))
                        .calledWithLiteralAt(
                                2,
                                Map.of(
                                        "c", "INSERT",
                                        "u", "UPDATE_AFTER",
                                        "d", "DELETE"))
                        .expectErrorMessage("must include UPDATE_BEFORE"));
    }
}
