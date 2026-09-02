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

package org.apache.flink.table.planner.plan;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.WindowTableFunctionQueryOperation;
import org.apache.flink.table.operations.WindowTableFunctionQueryOperation.WindowKind;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that a programmatically built {@link WindowTableFunctionQueryOperation} is lowered by
 * {@code QueryOperationConverter} to the same window-TVF plan as the equivalent {@code
 * TABLE(TUMBLE(...))} / {@code HOP} / {@code CUMULATE} / {@code SESSION} SQL.
 */
class WindowTableFunctionRelNodeTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TEMPORARY TABLE src (\n"
                                + "  id INT, amount INT, ts TIMESTAMP(3),\n"
                                + "  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND\n"
                                + ") WITH ('connector' = 'datagen')");
    }

    @Test
    void testTumbleRelPlan() {
        final Table table =
                tEnv().createTable(
                                tEnv().getOperationTreeBuilder()
                                        .windowTableFunction(
                                                WindowKind.TUMBLE,
                                                $("ts"),
                                                List.of(lit(10).minutes()),
                                                tEnv().from("src").getQueryOperation()));
        util.verifyRelPlan(table);
    }

    @Test
    void testHopRelPlan() {
        final Table table =
                tEnv().createTable(
                                tEnv().getOperationTreeBuilder()
                                        .windowTableFunction(
                                                WindowKind.HOP,
                                                $("ts"),
                                                List.of(lit(5).minutes(), lit(10).minutes()),
                                                tEnv().from("src").getQueryOperation()));
        util.verifyRelPlan(table);
    }

    @Test
    void testCumulateRelPlan() {
        final Table table =
                tEnv().createTable(
                                tEnv().getOperationTreeBuilder()
                                        .windowTableFunction(
                                                WindowKind.CUMULATE,
                                                $("ts"),
                                                List.of(lit(5).minutes(), lit(10).minutes()),
                                                tEnv().from("src").getQueryOperation()));
        util.verifyRelPlan(table);
    }

    @Test
    void testSessionRelPlan() {
        final Table table =
                tEnv().createTable(
                                tEnv().getOperationTreeBuilder()
                                        .windowTableFunction(
                                                WindowKind.SESSION,
                                                $("ts"),
                                                List.of(lit(10).minutes()),
                                                tEnv().from("src").getQueryOperation()));
        util.verifyRelPlan(table);
    }

    @Test
    void testYearMonthIntervalRejected() {
        assertThatThrownBy(
                        () ->
                                tEnv().getOperationTreeBuilder()
                                        .windowTableFunction(
                                                WindowKind.TUMBLE,
                                                $("ts"),
                                                List.of(lit(1).month()),
                                                tEnv().from("src").getQueryOperation()))
                .hasMessageContaining("day-time");
    }

    @Test
    void testZeroIntervalRejected() {
        assertThatThrownBy(
                        () ->
                                tEnv().getOperationTreeBuilder()
                                        .windowTableFunction(
                                                WindowKind.TUMBLE,
                                                $("ts"),
                                                List.of(lit(0).seconds()),
                                                tEnv().from("src").getQueryOperation()))
                .hasMessageContaining("positive");
    }

    @Test
    void testNegativeIntervalRejected() {
        assertThatThrownBy(
                        () ->
                                tEnv().getOperationTreeBuilder()
                                        .windowTableFunction(
                                                WindowKind.TUMBLE,
                                                $("ts"),
                                                List.of(lit(-5).seconds()),
                                                tEnv().from("src").getQueryOperation()))
                .hasMessageContaining("positive");
    }

    @Test
    void testNonLiteralIntervalRejected() {
        assertThatThrownBy(
                        () ->
                                tEnv().getOperationTreeBuilder()
                                        .windowTableFunction(
                                                WindowKind.TUMBLE,
                                                $("ts"),
                                                List.of($("ts")),
                                                tEnv().from("src").getQueryOperation()))
                .hasMessageContaining("interval literal");
    }

    private TableEnvironmentImpl tEnv() {
        return (TableEnvironmentImpl) util.tableEnv();
    }
}
