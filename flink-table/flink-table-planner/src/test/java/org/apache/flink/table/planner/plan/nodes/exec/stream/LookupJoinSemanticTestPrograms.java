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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** {@link TableTestProgram} definitions for semantic testing of lookup joins. */
public class LookupJoinSemanticTestPrograms {

    private static final SinkTestStep SINK =
            SinkTestStep.newBuilder("sink_t")
                    .addMode(ChangelogMode.upsert())
                    .addSchema("user_id INT PRIMARY KEY NOT ENFORCED", "amount INT")
                    .testMaterializedData()
                    .consumedValues("+I[2, 100]")
                    .build();

    static final TableTestProgram LOOKUP_TABLE_FILTER_WITH_UPDATED_JOIN_KEY =
            TableTestProgram.of(
                            "lookup-table-filter-with-updated-join-key",
                            "validates an update to a join key whose row the lookup table filter drops")
                    .setupTableSource(
                            lookupTable("regions_t", "region_id INT PRIMARY KEY NOT ENFORCED"))
                    .setupTableSource(
                            users(
                                    Row.ofKind(RowKind.INSERT, 1, 10, 100),
                                    Row.ofKind(RowKind.UPDATE_BEFORE, 1, 10, 100),
                                    Row.ofKind(RowKind.UPDATE_AFTER, 1, 20, 100),
                                    Row.ofKind(RowKind.INSERT, 2, 10, 100)))
                    .setupTableSink(SINK)
                    .runSql(
                            "INSERT INTO sink_t SELECT u.user_id, u.amount FROM users_t AS u "
                                    + "JOIN regions_t FOR SYSTEM_TIME AS OF u.proc_time AS r "
                                    + "ON u.region_id = r.region_id WHERE r.min_amount < 100")
                    .build();

    static final TableTestProgram NON_EQUI_CONDITION_WITH_UPDATED_COLUMN =
            TableTestProgram.of(
                            "non-equi-condition-with-updated-column",
                            "validates an update to a column that the join condition then rejects")
                    .setupTableSource(
                            lookupTable("limits_t", "user_id INT PRIMARY KEY NOT ENFORCED"))
                    .setupTableSource(
                            users(
                                    Row.ofKind(RowKind.INSERT, 1, 10, 100),
                                    Row.ofKind(RowKind.UPDATE_BEFORE, 1, 10, 100),
                                    Row.ofKind(RowKind.UPDATE_AFTER, 1, 10, 10),
                                    Row.ofKind(RowKind.INSERT, 2, 10, 100)))
                    .setupTableSink(SINK)
                    .runSql(
                            "INSERT INTO sink_t SELECT u.user_id, u.amount FROM users_t AS u "
                                    + "JOIN limits_t FOR SYSTEM_TIME AS OF u.proc_time AS l "
                                    + "ON u.user_id = l.user_id AND u.amount > l.min_amount")
                    .build();

    private static SourceTestStep lookupTable(String name, String key) {
        return SourceTestStep.newBuilder(name)
                .addSchema(key, "min_amount INT")
                .producedValues(Row.of(1, 50), Row.of(2, 50), Row.of(10, 50), Row.of(20, 500))
                .build();
    }

    private static SourceTestStep users(Row... rows) {
        return SourceTestStep.newBuilder("users_t")
                .addMode(ChangelogMode.all())
                .addSchema(
                        "user_id INT PRIMARY KEY NOT ENFORCED",
                        "region_id INT",
                        "amount INT",
                        "proc_time AS PROCTIME()")
                .producedValues(rows)
                .build();
    }
}
