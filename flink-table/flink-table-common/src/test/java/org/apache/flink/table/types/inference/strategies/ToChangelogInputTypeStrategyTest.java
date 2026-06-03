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

import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.TO_CHANGELOG_INPUT_TYPE_STRATEGY;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_OP;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_OP_MAPPING;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_PRODUCES_FULL_DELETES;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_TABLE;

/** Tests for {@link ToChangelogTypeStrategy#INPUT_TYPE_STRATEGY}. */
class ToChangelogInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final DataType TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("score", DataTypes.BIGINT()));

    private static final DataType DESCRIPTOR_TYPE = DataTypes.DESCRIPTOR();

    private static final DataType MAP_TYPE = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());

    private static final DataType BOOLEAN_TYPE = DataTypes.BOOLEAN();

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // Valid: produces_full_deletes=true with default op_mapping (includes DELETE)
                TestSpec.forStrategy(
                                "Valid produces_full_deletes=true with default mapping",
                                TO_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(ARG_TABLE, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(ARG_OP_MAPPING, null)
                        .calledWithLiteralAt(ARG_PRODUCES_FULL_DELETES, true)
                        .expectArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE),

                // Valid: produces_full_deletes=true with op_mapping that includes DELETE
                TestSpec.forStrategy(
                                "Valid produces_full_deletes=true with explicit DELETE mapping",
                                TO_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(ARG_TABLE, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(ARG_OP_MAPPING, Map.of("INSERT", "I", "DELETE", "D"))
                        .calledWithLiteralAt(ARG_PRODUCES_FULL_DELETES, true)
                        .expectArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE),

                // Valid: produces_full_deletes=true with comma-separated DELETE key
                TestSpec.forStrategy(
                                "Valid produces_full_deletes=true with comma-separated DELETE",
                                TO_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(ARG_TABLE, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(ARG_OP_MAPPING, Map.of("INSERT, DELETE", "X"))
                        .calledWithLiteralAt(ARG_PRODUCES_FULL_DELETES, true)
                        .expectArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE),

                // Valid: produces_full_deletes=false with op_mapping that omits DELETE
                TestSpec.forStrategy(
                                "Valid produces_full_deletes=false with no DELETE in mapping",
                                TO_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(ARG_TABLE, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(ARG_OP_MAPPING, Map.of("INSERT, UPDATE_AFTER", "X"))
                        .calledWithLiteralAt(ARG_PRODUCES_FULL_DELETES, false)
                        .expectArgumentTypes(TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE),

                // Error: multi-column descriptor for `op`
                TestSpec.forStrategy(
                                "Descriptor with multiple columns",
                                TO_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(ARG_TABLE, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("a", "b"))
                        .expectErrorMessage("must contain exactly one column name"),

                // Error: invalid RowKind in op_mapping key
                TestSpec.forStrategy(
                                "Invalid RowKind in mapping key", TO_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(ARG_TABLE, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(ARG_OP_MAPPING, Map.of("INVALID_KIND", "X"))
                        .expectErrorMessage("Unknown change operation: 'INVALID_KIND'"),

                // Error: duplicate RowKind across entries
                TestSpec.forStrategy(
                                "Duplicate RowKind in mapping keys",
                                TO_CHANGELOG_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(ARG_TABLE, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(
                                ARG_OP_MAPPING, Map.of("INSERT, DELETE", "A", "DELETE", "B"))
                        .expectErrorMessage("Duplicate change operation: 'DELETE'"));
    }
}
