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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan tests for the SNAPSHOT built-in process table function used by the LATERAL SNAPSHOT temporal
 * join (FLIP-579).
 *
 * <p>These tests focus on the function surface: that a call parses, that its named arguments pass
 * type inference, that PARTITION BY and the implicit system arguments are rejected, and that a
 * {@code LATERAL} use is rewritten into the dedicated LATERAL SNAPSHOT join. End-to-end join
 * translation and semantics are covered by {@code LateralSnapshotJoinTest}.
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
                                + "  rate_time TIMESTAMP(3),"
                                + "  WATERMARK FOR rate_time AS rate_time"
                                + ") WITH ('connector' = 'values')");
    }

    @Test
    void testLateralContext() {
        // SNAPSHOT used in a LATERAL context is rewritten into the dedicated LATERAL SNAPSHOT join.
        // An explicit load_completed_time keeps the rewrite deterministic; we only assert that the
        // dedicated join node appears rather than pinning the whole plan.
        final String plan =
                util.tableEnv()
                        .explainSql(
                                "SELECT o.order_id, o.amount, r.rate "
                                        + "FROM Orders AS o, LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE Rates, "
                                        + "load_completed_condition => 'user_time', "
                                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                                        + ")) AS r "
                                        + "WHERE o.currency = r.currency");
        assertThat(plan).contains("LateralSnapshotJoin");
    }

    @Test
    void testLateralContextInView() {
        // A view stores the expanded query (as do materialized tables). Selecting from a view whose
        // body uses LATERAL SNAPSHOT must still be rewritten into the dedicated join.
        util.tableEnv()
                .executeSql(
                        "CREATE VIEW OrdersWithRate AS "
                                + "SELECT o.order_id, o.amount, r.rate "
                                + "FROM Orders AS o, LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE Rates, "
                                + "load_completed_condition => 'user_time', "
                                + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                                + ")) AS r "
                                + "WHERE o.currency = r.currency");
        final String plan = util.tableEnv().explainSql("SELECT * FROM OrdersWithRate");
        assertThat(plan).contains("LateralSnapshotJoin");
    }

    @Test
    void testSnapshotWithViewArgument() {
        util.tableEnv().executeSql("CREATE VIEW RatesView AS SELECT * FROM Rates");
        final String plan =
                util.tableEnv()
                        .explainSql(
                                "SELECT o.order_id, o.amount, r.rate "
                                        + "FROM Orders AS o, LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE RatesView, "
                                        + "load_completed_condition => 'user_time', "
                                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                                        + ")) AS r "
                                        + "WHERE o.currency = r.currency");
        assertThat(plan).contains("LateralSnapshotJoin");
    }

    @Test
    @Disabled(
            "SNAPSHOT sets disableSystemArguments(true), but that flag is currently not enforced. "
                    + "Re-enable once FLINK-40079 is fixed.")
    void testSystemArgumentsNotAllowed() {
        // SNAPSHOT disables the implicit system arguments (e.g. `on_time`). Passing one in a
        // LATERAL context must be rejected because the argument is not part of the function
        // signature.
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT o.order_id "
                                                + "FROM Orders AS o, LATERAL TABLE(SNAPSHOT("
                                                + "input => TABLE Rates, "
                                                + "on_time => DESCRIPTOR(rate_time))) AS r "
                                                + "WHERE o.currency = r.currency"))
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
    void testFromContextRejectedOutsideLateral() {
        // SNAPSHOT used outside a LATERAL context is not rewritten by the LATERAL SNAPSHOT rule.
        // ForbidSnapshotOutsideLateralRule intercepts the surviving SNAPSHOT scan and rejects it
        // with a clear message before it reaches the generic PTF translation.
        assertThatThrownBy(() -> util.verifyRelPlan("SELECT * FROM SNAPSHOT(input => TABLE Rates)"))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "The SNAPSHOT function can only be used as the build side "
                                        + "(right-hand side) of a LATERAL join"));
    }
}
