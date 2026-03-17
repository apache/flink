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
import org.junit.jupiter.api.Test;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the TO_CHANGELOG built-in process table function. */
public class ToChangelogTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  val BIGINT"
                                + ") WITH ('connector' = 'values')");
        util.tableEnv()
                .executeSql(
                        "CREATE VIEW t_updating AS SELECT id, name, COUNT(*) AS cnt FROM t GROUP BY id, name");
    }

    @Test
    void testDefaultCall() {
        util.verifyRelPlan("SELECT * FROM TO_CHANGELOG(input => TABLE t_updating PARTITION BY id)");
    }

    @Test
    void testCustomOpName() {
        util.verifyRelPlan(
                "SELECT * FROM TO_CHANGELOG("
                        + "input => TABLE t_updating PARTITION BY id, "
                        + "op => DESCRIPTOR(op_code))");
    }

    @Test
    void testCustomOpMapping() {
        util.verifyRelPlan(
                "SELECT * FROM TO_CHANGELOG("
                        + "input => TABLE t_updating PARTITION BY id, "
                        + "op => DESCRIPTOR(op_code), "
                        + "op_mapping => MAP['INSERT','I', 'DELETE','D', 'UPDATE_AFTER','U'])");
    }

    @Test
    void testMultiplePartitionKeys() {
        util.verifyRelPlan(
                "SELECT * FROM TO_CHANGELOG(input => TABLE t_updating PARTITION BY (id, name))");
    }

    @Test
    void testMissingPartitionBy() {
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
