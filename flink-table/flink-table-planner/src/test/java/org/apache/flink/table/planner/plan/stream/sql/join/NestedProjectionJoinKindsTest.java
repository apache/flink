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

package org.apache.flink.table.planner.plan.stream.sql.join;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

/**
 * Plan tests for joins that access nested fields, planned with the production optimizer. They check
 * that {@code ProjectNestedFieldJoinTransposeRule} keeps the special joins (temporal, interval,
 * lateral snapshot) intact and that it does not leave a join input without fields.
 */
class NestedProjectionJoinKindsTest extends TableTestBase {

    private static final String NESTED = "nested ROW<a INT, b STRING, c INT>";
    private static final String WATERMARK = "WATERMARK FOR ts AS ts - INTERVAL '1' SECOND";
    private static final String OPTIONS =
            "'connector' = 'values', 'bounded' = 'false', 'disable-lookup' = 'true', "
                    + "'nested-projection-supported' = 'true'";

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @BeforeEach
    void setup() {
        // the load completed time of the lateral snapshot join depends on the time zone
        util.tableEnv().getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE Orders (id INT, %s, ts TIMESTAMP(3), %s) WITH (%s)",
                                NESTED, WATERMARK, OPTIONS));
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE Orders2 (id INT, %s, ts TIMESTAMP(3), %s) WITH (%s)",
                                NESTED, WATERMARK, OPTIONS));
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE Rates (id INT, %s, ts TIMESTAMP(3), %s, "
                                        + "PRIMARY KEY (id) NOT ENFORCED) "
                                        + "WITH (%s, 'changelog-mode' = 'I,UA,D')",
                                NESTED, WATERMARK, OPTIONS));
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE Build (id INT, %s, ts TIMESTAMP(3), %s) WITH (%s)",
                                NESTED, WATERMARK, OPTIONS));
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE L (id INT, nested ROW<name STRING, v INT>) "
                                + "WITH ('connector' = 'values', 'bounded' = 'false')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE R (id INT) "
                                + "WITH ('connector' = 'values', 'bounded' = 'false')");
    }

    @Test
    void testEventTimeTemporalJoin() {
        util.verifyRelPlan(
                "SELECT Orders.nested.a, Rates.nested.b FROM Orders JOIN Rates "
                        + "FOR SYSTEM_TIME AS OF Orders.ts ON Orders.id = Rates.id");
    }

    @Test
    void testIntervalJoin() {
        util.verifyRelPlan(
                "SELECT Orders.nested.a, Orders2.nested.b FROM Orders JOIN Orders2 "
                        + "ON Orders.id = Orders2.id AND Orders.ts "
                        + "BETWEEN Orders2.ts - INTERVAL '1' MINUTE AND Orders2.ts");
    }

    @Test
    void testLateralSnapshotJoin() {
        util.verifyRelPlan(
                "SELECT Orders.nested.a, s.nested.b FROM Orders JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE Build, on_time => DESCRIPTOR(ts), "
                        + "load_completed_time => CAST(TIMESTAMP '2020-01-01 00:00:10' "
                        + "AS TIMESTAMP_LTZ(3)))) AS s ON Orders.id = s.id");
    }

    @Test
    void testOuterJoinWithoutReferencedColumnsOfOneInput() {
        // no column of R is needed, and the pushed leaf is dropped by the outer SELECT afterwards:
        // the join input R must keep a column, or FlinkProjectJoinTransposeRule fails on it
        util.verifyRelPlan(
                "SELECT a FROM (SELECT L.id AS a, L.nested.name AS n FROM L "
                        + "LEFT JOIN R ON L.id > 10) WHERE n = 'x'");
    }
}
