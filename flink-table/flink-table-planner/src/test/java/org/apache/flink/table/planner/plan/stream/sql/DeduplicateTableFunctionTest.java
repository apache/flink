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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.PlanKind;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import scala.Enumeration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.descriptor;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Plan tests for the changelog mode and upsert key of the DEDUPLICATE built-in PTF. */
class DeduplicateTableFunctionTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE user_events (user_id STRING, event STRING, "
                                + "ts TIMESTAMP_LTZ(3), WATERMARK FOR ts AS ts) "
                                + "WITH ('connector' = 'values')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE customer_changes (customer_id STRING, city STRING, "
                                + "lsn BIGINT) WITH ('connector' = 'values')");
    }

    @Test
    void testUpsertKeyIsPartitionKey() {
        verifyPlan(
                "SELECT * FROM DEDUPLICATE("
                        + "input => TABLE user_events PARTITION BY user_id, keep => 'last')");
    }

    @Test
    void testRetractingConsumer() {
        verifyPlan(
                "SELECT event, COUNT(*) AS users FROM DEDUPLICATE("
                        + "input => TABLE user_events PARTITION BY user_id, keep => 'last') "
                        + "GROUP BY event");
    }

    @Test
    void testFirstArrivalIsInsertOnly() {
        verifyPlan(
                "SELECT * FROM DEDUPLICATE("
                        + "input => TABLE customer_changes PARTITION BY customer_id)");
    }

    @Test
    void testOnTimeAddsRowtimeAttribute() {
        verifyPlan(
                "SELECT * FROM DEDUPLICATE("
                        + "input => TABLE user_events PARTITION BY user_id, "
                        + "on_time => DESCRIPTOR(ts))");
    }

    @Test
    void testOnTimeWithKeepLastIsRejected() {
        assertThatThrownBy(
                        () ->
                                util.verifyExecPlan(
                                        "SELECT * FROM DEDUPLICATE("
                                                + "input => TABLE user_events PARTITION BY user_id, "
                                                + "keep => 'last', on_time => DESCRIPTOR(ts))"))
                .hasMessageContaining(
                        "Time operations using the `on_time` argument are currently not "
                                + "supported for PTFs that consume or produce updates.");
    }

    @Test
    void testFirstWithSequenceByIsUpdating() {
        verifyPlan(
                "SELECT * FROM DEDUPLICATE("
                        + "input => TABLE customer_changes PARTITION BY customer_id, "
                        + "sequence_by => DESCRIPTOR(lsn))");
    }

    @Test
    void testTableApiWithArguments() {
        final Table result =
                util.tableEnv()
                        .from("customer_changes")
                        .partitionBy($("customer_id"))
                        .deduplicate(
                                lit("last").asArgument("keep"),
                                descriptor("lsn").asArgument("sequence_by"),
                                lit(Duration.ofDays(1)).asArgument("state_ttl"));
        util.verifyRelPlan(result);
    }

    private void verifyPlan(final String query) {
        util.doVerifyPlan(
                query,
                new ExplainDetail[] {ExplainDetail.CHANGELOG_MODE},
                false,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_REL()},
                false,
                true);
    }
}
