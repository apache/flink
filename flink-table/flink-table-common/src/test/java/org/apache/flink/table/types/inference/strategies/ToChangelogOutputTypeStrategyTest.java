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
import org.apache.flink.types.ColumnList;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.SpecificTypeStrategies.TO_CHANGELOG_OUTPUT_TYPE_STRATEGY;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_OP;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_OP_MAPPING;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_PRODUCES_FULL_DELETES;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_TABLE;

/** Tests for {@link ToChangelogTypeStrategy#OUTPUT_TYPE_STRATEGY}. */
class ToChangelogOutputTypeStrategyTest extends TypeStrategiesTestBase {

    private static final DataType TABLE_TYPE_NOT_NULL_SCORE =
            DataTypes.ROW(
                    DataTypes.FIELD("name", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("score", DataTypes.BIGINT().notNull()));

    private static final DataType DESCRIPTOR_TYPE = DataTypes.DESCRIPTOR();
    private static final DataType MAP_TYPE = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
    private static final DataType BOOLEAN_TYPE = DataTypes.BOOLEAN();

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(
                                "produces_full_deletes=true preserves NOT NULL on input columns",
                                TO_CHANGELOG_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(
                                TABLE_TYPE_NOT_NULL_SCORE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(
                                ARG_TABLE, new TableSemanticsMock(TABLE_TYPE_NOT_NULL_SCORE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(ARG_OP_MAPPING, null)
                        .calledWithLiteralAt(ARG_PRODUCES_FULL_DELETES, true)
                        .expectDataType(
                                DataTypes.ROW(
                                                DataTypes.FIELD("op", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "name", DataTypes.STRING().notNull()),
                                                DataTypes.FIELD(
                                                        "score", DataTypes.BIGINT().notNull()))
                                        .notNull()),
                TestSpec.forStrategy(
                                "produces_full_deletes=false widens non-upsert-key columns to nullable",
                                TO_CHANGELOG_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(
                                TABLE_TYPE_NOT_NULL_SCORE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(
                                ARG_TABLE,
                                new TableSemanticsMock(
                                        TABLE_TYPE_NOT_NULL_SCORE,
                                        new int[0],
                                        new int[0],
                                        -1,
                                        null,
                                        List.of(new int[] {0})))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(ARG_OP_MAPPING, null)
                        .calledWithLiteralAt(ARG_PRODUCES_FULL_DELETES, false)
                        .expectDataType(
                                DataTypes.ROW(
                                                DataTypes.FIELD("op", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "name", DataTypes.STRING().notNull()),
                                                DataTypes.FIELD("score", DataTypes.BIGINT()))
                                        .notNull()),
                TestSpec.forStrategy(
                                "produces_full_deletes=false without upsert key widens all columns",
                                TO_CHANGELOG_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(
                                TABLE_TYPE_NOT_NULL_SCORE, DESCRIPTOR_TYPE, MAP_TYPE, BOOLEAN_TYPE)
                        .calledWithTableSemanticsAt(
                                ARG_TABLE, new TableSemanticsMock(TABLE_TYPE_NOT_NULL_SCORE))
                        .calledWithLiteralAt(ARG_OP, ColumnList.of("op"))
                        .calledWithLiteralAt(ARG_OP_MAPPING, null)
                        .calledWithLiteralAt(ARG_PRODUCES_FULL_DELETES, false)
                        .expectDataType(
                                DataTypes.ROW(
                                                DataTypes.FIELD("op", DataTypes.STRING()),
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.BIGINT()))
                                        .notNull()));
    }
}
