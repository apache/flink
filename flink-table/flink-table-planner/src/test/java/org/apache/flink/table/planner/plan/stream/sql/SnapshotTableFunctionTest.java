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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan tests for the SNAPSHOT built-in process table function used by the LATERAL SNAPSHOT temporal
 * join (FLIP-579).
 *
 * <p>SNAPSHOT is a planner placeholder without a runtime implementation. These tests therefore only
 * verify that a call parses, that its named arguments pass type inference, and that it survives
 * logical optimization in both a plain {@code FROM} clause and a {@code LATERAL} context. Verifying
 * the optimized rel plan (rather than the exec plan) stops short of the runtime translation that a
 * future optimizer rule will provide by rewriting the call into a dedicated temporal-join operator.
 */
public class SnapshotTableFunctionTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        // Probe side of the temporal join.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Orders ("
                                + "  order_id INT,"
                                + "  currency STRING,"
                                + "  amount INT,"
                                + "  order_time TIMESTAMP(3),"
                                + "  WATERMARK FOR order_time AS order_time"
                                + ") WITH ('connector' = 'datagen')");
        // Build side: the updating dimension table that SNAPSHOT takes as its table argument.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Rates ("
                                + "  currency STRING,"
                                + "  rate INT,"
                                + "  rate_time TIMESTAMP(3)"
                                + ") WITH ('connector' = 'values')");
        // Sinks used by the execution tests below.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE RatesSink ("
                                + "  currency STRING,"
                                + "  rate INT,"
                                + "  rate_time TIMESTAMP(3)"
                                + ") WITH ('connector' = 'blackhole')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE JoinSink ("
                                + "  order_id INT,"
                                + "  amount INT,"
                                + "  rate INT"
                                + ") WITH ('connector' = 'blackhole')");
    }

    @Test
    void testFromContext() {
        // SNAPSHOT used as a standalone table function with the full set of named arguments
        util.verifyRelPlan(
                "SELECT * FROM SNAPSHOT("
                        + "input => TABLE Rates, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00.001' AS TIMESTAMP_LTZ(3)), "
                        + "load_completed_idle_timeout => INTERVAL '10' SECOND, "
                        + "state_ttl => INTERVAL '1' DAY)");
    }

    @Test
    void testLateralContext() {
        // SNAPSHOT used in a LATERAL context
        util.verifyRelPlan(
                "SELECT o.order_id, o.amount, r.rate "
                        + "FROM Orders AS o, LATERAL TABLE(SNAPSHOT(input => TABLE Rates)) AS r "
                        + "WHERE o.currency = r.currency");
    }

    @Test
    @Disabled(
            "SNAPSHOT should disable the implicit system arguments (on_time, uid), but that is not "
                    + "wired up yet. disableSystemArguments(true) is only legal for a PTF that is "
                    + "rewritten by its own optimizer rule before reaching "
                    + "StreamPhysicalProcessTableFunctionRule (which otherwise rejects it with "
                    + "'Disabling system arguments is not supported for user-defined PTF').")
    void testSystemArgumentsNotAllowed() {
        // SNAPSHOT must disable the implicit system arguments (e.g. `on_time`). Passing one must be
        // rejected because the argument is not allowed.
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT * FROM SNAPSHOT("
                                                + "input => TABLE Rates, "
                                                + "on_time => DESCRIPTOR(rate_time))"))
                .satisfies(anyCauseMatches("on_time"));
    }

    @Test
    void testPartitionByNotAllowed() {
        // The table argument uses row semantics, so it does not accept a PARTITION BY clause.
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT * FROM SNAPSHOT("
                                                + "input => TABLE Rates PARTITION BY currency)"))
                .satisfies(
                        anyCauseMatches(
                                "Only tables with set semantics may be partitioned. Invalid PARTITION BY clause in the 0-th operand of table function 'SNAPSHOT'"));
    }

    @Test
    void testFromContextExecutionFails() {
        // SNAPSHOT has no runtime implementation yet, so compiling the query into the job's
        // transformations fails. A future optimizer rule is expected to rewrite the call before
        // this stage.
        assertThatThrownBy(
                        () ->
                                util.generateTransformations(
                                        "INSERT INTO RatesSink "
                                                + "SELECT * FROM SNAPSHOT(input => TABLE Rates)"))
                .satisfies(
                        anyCauseMatches(
                                "Could not find a runtime implementation for built-in function 'SNAPSHOT'. "
                                        + "The planner should have provided an implementation."));
    }

    @Test
    void testLateralContextExecutionFails() {
        // SNAPSHOT has no runtime implementation yet, so compiling the query into the job's
        // transformations fails. A future optimizer rule is expected to rewrite the call before
        // this stage.
        assertThatThrownBy(
                        () ->
                                util.generateTransformations(
                                        "INSERT INTO JoinSink "
                                                + "SELECT o.order_id, o.amount, r.rate "
                                                + "FROM Orders AS o, LATERAL TABLE(SNAPSHOT(input => TABLE Rates)) AS r "
                                                + "WHERE o.currency = r.currency"))
                .satisfies(
                        anyCauseMatches(
                                "Could not find a runtime implementation for built-in function 'SNAPSHOT'. "
                                        + "The planner should have provided an implementation."));
    }
}
