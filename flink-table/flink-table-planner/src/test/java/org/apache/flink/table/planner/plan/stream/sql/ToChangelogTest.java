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

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan tests for the TO_CHANGELOG built-in process table function. Uses {@link
 * ExplainDetail#CHANGELOG_MODE} to verify changelog mode propagation through the plan.
 */
public class ToChangelogTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
    }

    @Test
    void testRetractSource() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE retract_source ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UB,UA,D'"
                                + ")");
        util.verifyRelPlan(
                "SELECT * FROM TO_CHANGELOG(input => TABLE retract_source PARTITION BY id)",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.CHANGELOG_MODE)));
    }

    @Test
    void testUpsertSource() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsert_source ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UA,D'"
                                + ")");
        util.verifyRelPlan(
                "SELECT * FROM TO_CHANGELOG(input => TABLE upsert_source PARTITION BY id)",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.CHANGELOG_MODE)));
    }

    @Test
    void testInsertOnlySource() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE insert_only_source ("
                                + "  id INT,"
                                + "  name STRING"
                                + ") WITH ('connector' = 'values')");
        util.verifyRelPlan(
                "SELECT * FROM TO_CHANGELOG(input => TABLE insert_only_source PARTITION BY id)",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.CHANGELOG_MODE)));
    }

    @Test
    void testMissingPartitionBy() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t ("
                                + "  id INT,"
                                + "  name STRING"
                                + ") WITH ('connector' = 'values')");
        util.tableEnv()
                .executeSql(
                        "CREATE VIEW t_updating AS SELECT id, name, COUNT(*) AS cnt FROM t GROUP BY id, name");
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT * FROM TO_CHANGELOG(input => TABLE t_updating)"))
                .satisfies(
                        anyCauseMatches(
                                "Table argument 'input' requires a PARTITION BY clause for parallel processing."));
    }

    @Test
    void testInvalidDescriptorMultipleColumns() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t ("
                                + "  id INT,"
                                + "  name STRING"
                                + ") WITH ('connector' = 'values')");
        util.tableEnv()
                .executeSql(
                        "CREATE VIEW t_updating AS SELECT id, name, COUNT(*) AS cnt FROM t GROUP BY id, name");
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT * FROM TO_CHANGELOG("
                                                + "input => TABLE t_updating PARTITION BY id, "
                                                + "op => DESCRIPTOR(a, b))"))
                .satisfies(
                        anyCauseMatches(
                                "The descriptor for argument 'op' must contain exactly one column name."));
    }

    @Test
    void testInvalidRowKindInOpMapping() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t ("
                                + "  id INT,"
                                + "  name STRING"
                                + ") WITH ('connector' = 'values')");
        util.tableEnv()
                .executeSql(
                        "CREATE VIEW t_updating AS SELECT id, name, COUNT(*) AS cnt FROM t GROUP BY id, name");
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT * FROM TO_CHANGELOG("
                                                + "input => TABLE t_updating PARTITION BY id, "
                                                + "op_mapping => MAP['INVALID_KIND', 'X'])"))
                .satisfies(
                        anyCauseMatches(
                                "Invalid target mapping for argument 'op_mapping'."));
    }
}
