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

package org.apache.flink.table.planner.runtime.batch.table;

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.callSql;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.withColumns;
import static org.assertj.core.api.Assertions.assertThat;

/** Execution tests for computed grouping expressions and aggregate output references. */
class ComputedGroupingITCase extends BatchTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAggregateAliasTakesPrecedenceOverInputField(boolean windowed) throws Exception {
        final Table source = source();
        final ApiExpression key = $("a").plus(1);
        final Table result =
                windowed
                        ? source.window(Tumble.over(lit(5).seconds()).on($("ts")).as("w"))
                                .groupBy($("w"), key)
                                .aggregate($("b").sum().as("a"))
                                .select($("a").plus(1).as("out"))
                        : source.groupBy(key)
                                .aggregate($("b").sum().as("a"))
                                .select($("a").plus(1).as("out"));

        assertResult(result, Row.of(11));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSqlExpressionUsesAggregateOutputField(boolean windowed) throws Exception {
        final Table source = source();
        final ApiExpression key = callSql("a + 1").times(2);
        final Table result =
                windowed
                        ? source.window(Tumble.over(lit(5).seconds()).on($("ts")).as("w"))
                                .groupBy($("w"), key)
                                .aggregate($("b").sum().as("a"))
                                .select(callSql("a + 1").times(2).as("out"))
                        : source.groupBy(key)
                                .aggregate($("b").sum().as("a"))
                                .select(callSql("a + 1").times(2).as("out"));

        assertResult(result, Row.of(22));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFlattenedGroupingKeepsComputedGroupingPosition(boolean windowed) throws Exception {
        final Table source = source().addColumns(Expressions.row($("a"), $("b")).as("r"));
        final ApiExpression key = $("a").plus(1);
        final Table result =
                windowed
                        ? source.window(Tumble.over(lit(5).seconds()).on($("ts")).as("w"))
                                .groupBy($("w"), $("r").flatten(), key)
                                .select(withColumns(1, 2), key.as("key"), $("b").sum().as("total"))
                        : source.groupBy($("r").flatten(), key)
                                .select(withColumns(1, 2), key.as("key"), $("b").sum().as("total"));

        assertResult(result, Row.of(1, 10, 2, 10));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGeneratedAggregateNameDoesNotShadowInputField(boolean windowed) throws Exception {
        final Table source = source().as("EXPR$0", "b", "ts");
        final ApiExpression key = $("EXPR$0").plus(1);
        final Table result =
                windowed
                        ? source.window(Tumble.over(lit(5).seconds()).on($("ts")).as("w"))
                                .groupBy($("w"), key)
                                .select(key.as("key"), $("b").sum().plus(1).as("out"))
                        : source.groupBy(key).select(key.as("key"), $("b").sum().plus(1).as("out"));

        assertResult(result, Row.of(2, 11));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGeneratedAggregateNameDoesNotShadowGroupingAlias(boolean windowed) throws Exception {
        final Table source = source();
        final ApiExpression key = $("a").plus(1).as("EXPR$0");
        final Table result =
                windowed
                        ? source.window(Tumble.over(lit(5).seconds()).on($("ts")).as("w"))
                                .groupBy($("w"), key)
                                .select($("EXPR$0"), $("b").sum().plus(1).as("out"))
                        : source.groupBy(key).select($("EXPR$0"), $("b").sum().plus(1).as("out"));

        assertResult(result, Row.of(2, 11));
    }

    @Test
    void testWindowPropertyNameDoesNotShadowAggregateAlias() throws Exception {
        final Table result =
                source().addColumns(lit(0).as("EXPR$0"))
                        .window(Tumble.over(lit(5).seconds()).on($("ts")).as("w"))
                        .groupBy($("w"), $("a").plus(1))
                        .aggregate($("b").sum().as("EXPR$1"))
                        .select($("EXPR$1"), $("w").start().as("start"));

        assertResult(result, Row.of(10, LocalDateTime.of(2020, 1, 1, 0, 0)));
    }

    private Table source() {
        env().setParallelism(1);
        return tEnv().fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.INT()),
                                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3))),
                        Row.of(1, 10, LocalDateTime.of(2020, 1, 1, 0, 0, 1)));
    }

    private static void assertResult(Table table, Row expected) throws Exception {
        try (CloseableIterator<Row> rows = table.execute().collect()) {
            assertThat(rows).toIterable().containsExactly(expected);
        }
    }
}
