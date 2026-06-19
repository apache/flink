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
                                    .testMaterializedData()
                                    .consumedValues("+I[test_diff]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT t2.ext.nested.nested1.nested2 FROM source_t2 t2 WHERE"
                                    + " NOT EXISTS (SELECT 1 FROM source_t1 t1 WHERE t1.ext.nested = t2.ext.nested.nested1.nested2)")
                    .build();

    // --- NOT NULL ROW field, join on nested field ---

    public static final TableTestProgram LEFT_JOIN_NOT_NULL_NESTED_ROW =
            TableTestProgram.of(
                            "left-join-not-null-nested-row",
                            "NOT NULL ROW field from non-preserved side of LEFT JOIN must be nullable")
                    .setupTableSource(
                            SourceTestStep.newBuilder("lj_nn_orders")
                                    .addSchema(
                                            "`order_id` BIGINT NOT NULL",
                                            "PRIMARY KEY (`order_id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L), Row.of(2L))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("lj_nn_details")
                                    .addSchema(
                                            "`r` ROW<`order_id` BIGINT NOT NULL, `name` STRING NOT NULL> NOT NULL",
                                            "PRIMARY KEY (`r`) NOT ENFORCED")
                                    .producedValues(Row.of(Row.of(1L, "first")))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("lj_nn_sink")
                                    .addSchema("order_id BIGINT", "detail_id BIGINT")
                                    .testMaterializedData()
                                    .consumedValues("+I[1, 1]", "+I[2, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO lj_nn_sink "
                                    + "SELECT a.order_id, b.r.order_id "
                                    + "FROM lj_nn_orders a LEFT JOIN lj_nn_details b "
                                    + "ON a.order_id = b.r.order_id")
                    .build();

    public static final TableTestProgram RIGHT_JOIN_NOT_NULL_NESTED_ROW =
            TableTestProgram.of(
                            "right-join-not-null-nested-row",
                            "NOT NULL ROW field from non-preserved side of RIGHT JOIN must be nullable")
                    .setupTableSource(
                            SourceTestStep.newBuilder("rj_nn_orders")
                                    .addSchema(
                                            "`order_id` BIGINT NOT NULL",
                                            "PRIMARY KEY (`order_id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L), Row.of(2L))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("rj_nn_details")
                                    .addSchema(
                                            "`r` ROW<`order_id` BIGINT NOT NULL, `name` STRING NOT NULL> NOT NULL",
                                            "PRIMARY KEY (`r`) NOT ENFORCED")
                                    .producedValues(Row.of(Row.of(1L, "first")))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("rj_nn_sink")
                                    .addSchema("detail_id BIGINT", "order_id BIGINT")
                                    .testMaterializedData()
                                    .consumedValues("+I[1, 1]", "+I[null, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO rj_nn_sink "
                                    + "SELECT b.r.order_id, a.order_id "
                                    + "FROM rj_nn_details b RIGHT JOIN rj_nn_orders a "
                                    + "ON a.order_id = b.r.order_id")
                    .build();

    public static final TableTestProgram FULL_JOIN_NOT_NULL_NESTED_ROW =
            TableTestProgram.of(
                            "full-join-not-null-nested-row",
                            "NOT NULL ROW fields from both sides of FULL JOIN must be nullable")
                    .setupTableSource(
                            SourceTestStep.newBuilder("fj_nn_left")
                                    .addSchema(
                                            "`r` ROW<`id` BIGINT NOT NULL> NOT NULL",
                                            "PRIMARY KEY (`r`) NOT ENFORCED")
                                    .producedValues(Row.of(Row.of(1L)), Row.of(Row.of(2L)))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("fj_nn_right")
                                    .addSchema(
                                            "`r` ROW<`id` BIGINT NOT NULL> NOT NULL",
                                            "PRIMARY KEY (`r`) NOT ENFORCED")
                                    .producedValues(Row.of(Row.of(2L)), Row.of(Row.of(3L)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("fj_nn_sink")
                                    .addSchema("left_id BIGINT", "right_id BIGINT")
                                    .testMaterializedData()
                                    .consumedValues("+I[1, null]", "+I[2, 2]", "+I[null, 3]")
                                    .build())
                    .runSql(
                            "INSERT INTO fj_nn_sink "
                                    + "SELECT a.r.id, b.r.id "
                                    + "FROM fj_nn_left a FULL JOIN fj_nn_right b "
                                    + "ON a.r.id = b.r.id")
                    .build();

    // --- nullable ROW field, join on nested field ---

    public static final TableTestProgram LEFT_JOIN_NULLABLE_NESTED_ROW =
            TableTestProgram.of(
                            "left-join-nullable-nested-row",
                            "nullable ROW field from non-preserved side of LEFT JOIN must be nullable")
                    .setupTableSource(
                            SourceTestStep.newBuilder("lj_n_orders")
                                    .addSchema(
                                            "`order_id` BIGINT NOT NULL",
                                            "PRIMARY KEY (`order_id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L), Row.of(2L))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("lj_n_details")
                                    .addSchema(
                                            "`id` BIGINT NOT NULL",
                                            "`r` ROW<`order_id` BIGINT NOT NULL, `name` STRING>",
                                            "PRIMARY KEY (`id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L, Row.of(1L, "first")))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("lj_n_sink")
                                    .addSchema("order_id BIGINT", "detail_id BIGINT")
                                    .testMaterializedData()
                                    .consumedValues("+I[1, 1]", "+I[2, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO lj_n_sink "
                                    + "SELECT a.order_id, b.r.order_id "
                                    + "FROM lj_n_orders a LEFT JOIN lj_n_details b "
                                    + "ON a.order_id = b.r.order_id")
                    .build();

    public static final TableTestProgram RIGHT_JOIN_NULLABLE_NESTED_ROW =
            TableTestProgram.of(
                            "right-join-nullable-nested-row",
                            "nullable ROW field from non-preserved side of RIGHT JOIN must be nullable")
                    .setupTableSource(
                            SourceTestStep.newBuilder("rj_n_orders")
                                    .addSchema(
                                            "`order_id` BIGINT NOT NULL",
                                            "PRIMARY KEY (`order_id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L), Row.of(2L))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("rj_n_details")
                                    .addSchema(
                                            "`id` BIGINT NOT NULL",
                                            "`r` ROW<`order_id` BIGINT NOT NULL, `name` STRING>",
                                            "PRIMARY KEY (`id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L, Row.of(1L, "first")))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("rj_n_sink")
                                    .addSchema("detail_id BIGINT", "order_id BIGINT")
                                    .testMaterializedData()
                                    .consumedValues("+I[1, 1]", "+I[null, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO rj_n_sink "
                                    + "SELECT b.r.order_id, a.order_id "
                                    + "FROM rj_n_details b RIGHT JOIN rj_n_orders a "
                                    + "ON a.order_id = b.r.order_id")
                    .build();

    public static final TableTestProgram FULL_JOIN_NULLABLE_NESTED_ROW =
            TableTestProgram.of(
                            "full-join-nullable-nested-row",
                            "nullable ROW fields from both sides of FULL JOIN must be nullable")
                    .setupTableSource(
                            SourceTestStep.newBuilder("fj_n_left")
                                    .addSchema(
                                            "`id` BIGINT NOT NULL",
                                            "`r` ROW<`id` BIGINT NOT NULL>",
                                            "PRIMARY KEY (`id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L, Row.of(1L)), Row.of(2L, Row.of(2L)))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("fj_n_right")
                                    .addSchema(
                                            "`id` BIGINT NOT NULL",
                                            "`r` ROW<`id` BIGINT NOT NULL>",
                                            "PRIMARY KEY (`id`) NOT ENFORCED")
                                    .producedValues(Row.of(2L, Row.of(2L)), Row.of(3L, Row.of(3L)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("fj_n_sink")
                                    .addSchema("left_id BIGINT", "right_id BIGINT")
                                    .testMaterializedData()
                                    .consumedValues("+I[1, null]", "+I[2, 2]", "+I[null, 3]")
                                    .build())
                    .runSql(
                            "INSERT INTO fj_n_sink "
                                    + "SELECT a.r.id, b.r.id "
                                    + "FROM fj_n_left a FULL JOIN fj_n_right b "
                                    + "ON a.r.id = b.r.id")
                    .build();
}
