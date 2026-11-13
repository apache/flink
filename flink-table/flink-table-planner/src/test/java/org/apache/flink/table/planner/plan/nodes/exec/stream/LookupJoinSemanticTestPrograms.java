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

    static final TableTestProgram NON_EQUI_CONDITION_WITH_UPDATED_COLUMN =
            TableTestProgram.of(
                            "non-equi-condition-with-updated-column",
                            "validates an update to a column that the join condition then rejects")
                    .setupTableSource(
                            SourceTestStep.newBuilder("limits_t")
                                    .addSchema(
                                            "user_id INT PRIMARY KEY NOT ENFORCED",
                                            "min_amount INT")
                                    .producedValues(Row.of(1, 50), Row.of(2, 50))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("users_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(
                                            "user_id INT PRIMARY KEY NOT ENFORCED",
                                            "amount INT",
                                            "proc_time AS PROCTIME()")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 100),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, 100),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 100))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addMode(ChangelogMode.upsert())
                                    .addSchema("user_id INT PRIMARY KEY NOT ENFORCED", "amount INT")
                                    .testMaterializedData()
                                    .consumedValues("+I[2, 100]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT u.user_id, u.amount FROM users_t AS u "
                                    + "JOIN limits_t FOR SYSTEM_TIME AS OF u.proc_time AS l "
                                    + "ON u.user_id = l.user_id AND u.amount > l.min_amount")
                    .build();
}
