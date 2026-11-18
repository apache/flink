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
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Plan tests for the TO_APPEND built-in process table function. */
class ToAppendTest extends TableTestBase {

    private static final List<ExplainDetail> CHANGELOG_MODE = List.of(ExplainDetail.CHANGELOG_MODE);

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsert_source ("
                                + "  id INT PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UA,D',"
                                + "  'source.produces-delete-by-key' = 'true'"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsert_source_with_full_deletes ("
                                + "  id INT PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UA,D'"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE retract_source ("
                                + "  id INT PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'changelog-mode' = 'I,UB,UA,D'"
                                + ")");
    }

    @Test
    void testUpsertSourceWithPartialDeletes() {
        util.verifyRelPlan("SELECT * FROM TO_APPEND(input => TABLE upsert_source)", CHANGELOG_MODE);
    }

    @Test
    void testUpsertSourceWithFullDeletes() {
        util.verifyRelPlan(
                "SELECT * FROM TO_APPEND(input => TABLE upsert_source_with_full_deletes)",
                CHANGELOG_MODE);
    }

    @Test
    void testRetractSource() {
        util.verifyRelPlan(
                "SELECT * FROM TO_APPEND(input => TABLE retract_source)", CHANGELOG_MODE);
    }

    @Test
    void testInputWithoutUpsertKey() {
        util.tableEnv().executeSql("CREATE VIEW names AS SELECT name FROM upsert_source");
        util.verifyRelPlan("SELECT * FROM TO_APPEND(input => TABLE names)", CHANGELOG_MODE);
    }
}
