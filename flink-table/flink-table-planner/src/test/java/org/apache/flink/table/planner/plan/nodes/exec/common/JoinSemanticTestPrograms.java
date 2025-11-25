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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.stream.IntStream;

/** {@link TableTestProgram} definitions for semantic testing {@link StreamExecJoin}. */
public class JoinSemanticTestPrograms {
    public static final TableTestProgram OUTER_JOIN_CHANGELOG_TEST =
            TableTestProgram.of("join-duplicate-emission-bug", "bug with CTE and left join")
                    .setupTableSource(
                            SourceTestStep.newBuilder("upsert_table_with_duplicates")
                                    .addSchema(
                                            "`execution_plan_id` VARCHAR(2147483647) NOT NULL",
                                            "`workflow_id` VARCHAR(2147483647) NOT NULL",
                                            "`event_section_id` VARCHAR(2147483647) NOT NULL",
                                            "CONSTRAINT `PRIMARY` PRIMARY KEY (`execution_plan_id`, `event_section_id`) NOT ENFORCED")
                                    .addOption("changelog-mode", "I, UA,D")
                                    .producedValues(
                                            IntStream.range(0, 13)
                                                    .mapToObj(
                                                            i ->
                                                                    Row.ofKind(
                                                                            RowKind.UPDATE_AFTER,
                                                                            "section_id_1",
                                                                            "section_id_2",
                                                                            "section_id_3"))
                                                    .toArray(Row[]::new))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("event_element_id STRING", "cnt BIGINT")
                                    .testMaterializedData()
                                    .consumedValues(Row.of("pk-1", 1), Row.of("pk-2", 1))
                                    .build())
                    .runSql(
                            "INSERT INTO sink WITH\n"
                                    + "    section_detail as (\n"
                                    + "        SELECT s.event_section_id\n"
                                    + "        \n"
                                    + "        FROM upsert_table_with_duplicates s\n"
                                    + "    ),\n"
                                    + "\n"
                                    + "    event_element as (\n"
                                    + "        SELECT\n"
                                    + "            ed.id as event_element_id\n"
                                    + "        FROM (\n"
                                    + "          SELECT\n"
                                    + "                 'pk-2' id,\n"
                                    + "                 'section_id_3' section_id\n"
                                    + "           UNION ALL\n"
                                    + "          SELECT\n"
                                    + "                 'pk-1' id,\n"
                                    + "                 'section_id_3' section_id\n"
                                    + "        ) ed  \n"
                                    + "        LEFT JOIN\n"
                                    + "            section_detail as s\n"
                                    + "            ON s.event_section_id = ed.section_id\n"
                                    + "    )\n"
                                    + "\n"
                                    + "SELECT  event_element_id, COUNT(*) cnt\n"
                                    + "FROM event_element\n"
                                    + "GROUP BY event_element_id")
                    .build();

    public static final TableTestProgram ANTI_JOIN_ON_NESTED =
            TableTestProgram.of("anti-join-on-nested", "anti join on nested fields")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t1")
                                    .addSchema("`ext` ROW<`nested` STRING NOT NULL>")
                                    .producedValues(Row.of(Row.of("test_same")))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t2")
                                    .addSchema(
                                            "`ext` ROW<`nested` ROW<`nested1` ROW<`nested2` STRING NOT NULL>>>")
                                    .producedValues(
                                            Row.of(Row.of(Row.of(Row.of("test_diff")))),
                                            Row.of(Row.of(Row.of(Row.of("test_same")))))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("output STRING")
                                    .consumedValues("+I[test_diff]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT t2.ext.nested.nested1.nested2 FROM source_t2 t2 WHERE"
                                    + " NOT EXISTS (SELECT 1 FROM source_t1 t1 WHERE t1.ext.nested = t2.ext.nested.nested1.nested2)")
                    .build();
}
