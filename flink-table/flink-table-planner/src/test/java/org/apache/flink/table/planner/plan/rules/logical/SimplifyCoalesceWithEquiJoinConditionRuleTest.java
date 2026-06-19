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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test rule {@link SimplifyCoalesceWithEquiJoinConditionRule}. */
class SimplifyCoalesceWithEquiJoinConditionRuleTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    void before() {
        util = streamTestUtil(TableConfig.getDefault());

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE orders ("
                                + "  order_id BIGINT NOT NULL,"
                                + "  user_id BIGINT NOT NULL,"
                                + "  amount DOUBLE,"
                                + "  PRIMARY KEY (order_id) NOT ENFORCED"
                                + ") WITH ('connector' = 'values')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE order_details ("
                                + "  order_id BIGINT NOT NULL,"
                                + "  detail STRING,"
                                + "  PRIMARY KEY (order_id) NOT ENFORCED"
                                + ") WITH ('connector' = 'values')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE composite_key_table ("
                                + "  k1 BIGINT NOT NULL,"
                                + "  k2 BIGINT NOT NULL,"
                                + "  val STRING,"
                                + "  PRIMARY KEY (k1, k2) NOT ENFORCED"
                                + ") WITH ('connector' = 'values')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE composite_key_details ("
                                + "  k1 BIGINT NOT NULL,"
                                + "  k2 BIGINT NOT NULL,"
                                + "  info STRING,"
                                + "  PRIMARY KEY (k1, k2) NOT ENFORCED"
                                + ") WITH ('connector' = 'values')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE order_details_row ("
                                + "  r ROW<order_id BIGINT NOT NULL, order_name STRING NOT NULL> NOT NULL, "
                                + "  detail STRING,"
                                + "  PRIMARY KEY (r) NOT ENFORCED"
                                + ") WITH ('connector' = 'values')");
    }

    @Test
    void testCoalesceOnLeftJoinEquiKey() {
        util.verifyRelPlan(
                "SELECT COALESCE(b.order_id, a.order_id) AS order_id, a.amount "
                        + "FROM orders a LEFT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceReversedArgsOnLeftJoin() {
        util.verifyRelPlan(
                "SELECT COALESCE(a.order_id, b.order_id) AS order_id, a.amount "
                        + "FROM orders a LEFT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceOnInnerJoinEquiKey() {
        util.verifyRelPlan(
                "SELECT COALESCE(b.order_id, a.order_id) AS order_id "
                        + "FROM orders a INNER JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceOnRightJoinEquiKey() {
        util.verifyRelPlan(
                "SELECT COALESCE(a.order_id, b.order_id) AS order_id "
                        + "FROM orders a RIGHT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceOnFullJoinNotSimplified() {
        util.verifyRelPlan(
                "SELECT COALESCE(b.order_id, a.order_id) AS order_id "
                        + "FROM orders a FULL JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceOnNonEquiColumnsNotSimplified() {
        util.verifyRelPlan(
                "SELECT COALESCE(b.detail, CAST(a.amount AS STRING)) AS val "
                        + "FROM orders a LEFT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceWithThreeArgs() {
        util.verifyRelPlan(
                "SELECT COALESCE(b.order_id, a.order_id, 0) AS order_id "
                        + "FROM orders a LEFT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testMultipleCoalesceOnCompositeKey() {
        util.verifyRelPlan(
                "SELECT COALESCE(b.k1, a.k1) AS k1, COALESCE(b.k2, a.k2) AS k2, a.val "
                        + "FROM composite_key_table a "
                        + "LEFT JOIN composite_key_details b ON a.k1 = b.k1 AND a.k2 = b.k2");
    }

    @Test
    void testCoalesceThreeArgsAdjacentPair() {
        util.verifyRelPlan(
                "SELECT COALESCE(b.order_id, a.order_id, a.user_id) AS val "
                        + "FROM orders a LEFT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceThreeArgsNonPreservedAfterPreserved() {
        util.verifyRelPlan(
                "SELECT COALESCE(a.user_id, a.order_id, b.order_id) AS val "
                        + "FROM orders a LEFT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceThreeArgsNonPreservedBeforeWithGapNotSimplified() {
        util.verifyRelPlan(
                "SELECT COALESCE(b.order_id, a.user_id, a.order_id) AS val "
                        + "FROM orders a LEFT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceThreeArgsInnerJoin() {
        util.verifyRelPlan(
                "SELECT COALESCE(a.user_id, b.order_id, a.order_id) AS val "
                        + "FROM orders a INNER JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceWrappedInCast() {
        util.verifyRelPlan(
                "SELECT CAST(COALESCE(b.order_id, a.order_id) AS STRING) AS order_id_str "
                        + "FROM orders a LEFT JOIN order_details b ON a.order_id = b.order_id");
    }

    @Test
    void testCoalesceOnNestedRowScalarField() {
        util.verifyRelPlan(
                "SELECT CAST(COALESCE(b.r.order_id, a.order_id) AS STRING) AS order_id_str "
                        + "FROM orders a LEFT JOIN order_details_row b ON a.order_id = b.r.order_id");
    }

    @Test
    void testNestedRowFieldAccessNullableAfterLeftJoin() {
        // b.r.order_id must be nullable after a LEFT JOIN even though
        // the ROW field is declared NOT NULL, because the ROW itself
        // is null-padded on the non-preserved side.
        Table table =
                util.tableEnv()
                        .sqlQuery(
                                "SELECT b.r.order_id "
                                        + "FROM orders a LEFT JOIN order_details_row b "
                                        + "ON a.order_id = b.r.order_id");
        RelNode relNode = TableTestUtil.toRelNode(table);
        LogicalProject project = (LogicalProject) relNode;
        RexNode expr = project.getProjects().get(0);
        assertThat(expr.getType().isNullable())
                .as("Field access on nullable ROW from LEFT JOIN should be nullable, got: " + expr)
                .isTrue();
    }
}
