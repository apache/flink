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

import java.time.LocalDateTime;

/** {@link TableTestProgram} definitions for testing the built-in TO_APPEND PTF. */
public class ToAppendTestPrograms {

    private static final SourceTestStep UPSERT_SOURCE =
            SourceTestStep.newBuilder("source_table")
                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v STRING")
                    .addMode(ChangelogMode.upsert(true))
                    .producedValues(
                            Row.ofKind(RowKind.INSERT, 1, "a"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "b"),
                            Row.ofKind(RowKind.DELETE, 1, null))
                    .build();

    public static final TableTestProgram UPSERT =
            TableTestProgram.of(
                            "to-append-upsert", "upserts become inserts and the delete is dropped")
                    .setupTableSource(UPSERT_SOURCE)
                    .setupTableSink(appendOnlySink("+I[1, a]", "+I[1, b]"))
                    .runSql("INSERT INTO sink SELECT * FROM TO_APPEND(input => TABLE source_table)")
                    .build();

    public static final TableTestProgram RETRACT =
            TableTestProgram.of(
                            "to-append-retract",
                            "inserts and update-afters become inserts, the rest is dropped")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_table")
                                    .addSchema("id INT", "v STRING")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "b"),
                                            Row.ofKind(RowKind.DELETE, 1, "b"))
                                    .build())
                    .setupTableSink(appendOnlySink("+I[1, a]", "+I[1, b]"))
                    .runSql("INSERT INTO sink SELECT * FROM TO_APPEND(input => TABLE source_table)")
                    .build();

    public static final TableTestProgram WITHOUT_KEY =
            TableTestProgram.of(
                            "to-append-without-key",
                            "the key is projected away and the key-only delete is dropped")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_table")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "tags ARRAY<STRING> NOT NULL")
                                    .addMode(ChangelogMode.upsert(true))
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "a", new String[] {"x"}),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    1,
                                                    "b",
                                                    new String[] {"x", "y"}),
                                            // Delete by key: NOT NULL array column is null
                                            Row.ofKind(RowKind.DELETE, 1, null, null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("r ROW<name STRING, tags ARRAY<STRING>>")
                                    .addMode(ChangelogMode.insertOnly())
                                    .consumedValues("+I[+I[a, [x]]]", "+I[+I[b, [x, y]]]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "WITH projected AS (SELECT ROW(name, tags) AS r FROM source_table) "
                                    + "SELECT * FROM TO_APPEND(input => TABLE projected)")
                    .build();

    public static final TableTestProgram UNION_ALL =
            TableTestProgram.of(
                            "to-append-union-all",
                            "upserts of both inputs become inserts and the deletes are dropped")
                    .setupTableSource(UPSERT_SOURCE)
                    .setupTableSource(
                            SourceTestStep.newBuilder("other_source_table")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v STRING")
                                    .addMode(ChangelogMode.upsert(false))
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 2, "c"),
                                            Row.ofKind(RowKind.DELETE, 2, "c"),
                                            Row.ofKind(RowKind.INSERT, 2, "d"))
                                    .build())
                    .setupTableSink(appendOnlySink("+I[1, a]", "+I[1, b]", "+I[2, c]", "+I[2, d]"))
                    .runSql(
                            "INSERT INTO sink "
                                    + "WITH all_rows AS ("
                                    + "SELECT * FROM source_table UNION ALL SELECT * FROM other_source_table) "
                                    + "SELECT * FROM TO_APPEND(input => TABLE all_rows)")
                    .build();

    public static final TableTestProgram WINDOW =
            TableTestProgram.of(
                            "to-append-window",
                            "Tumbling window over the changes of an upsert table")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(
                                            "order_id INT",
                                            "status STRING",
                                            "ts TIMESTAMP(3)",
                                            "PRIMARY KEY (order_id) NOT ENFORCED",
                                            "WATERMARK FOR ts AS ts")
                                    .addMode(ChangelogMode.upsert())
                                    .producedValues(
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    1,
                                                    "CREATED",
                                                    LocalDateTime.parse("2024-01-01T10:05")),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    2,
                                                    "CREATED",
                                                    LocalDateTime.parse("2024-01-01T10:20")),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    1,
                                                    "PAID",
                                                    LocalDateTime.parse("2024-01-01T10:40")),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    1,
                                                    "SHIPPED",
                                                    LocalDateTime.parse("2024-01-01T11:10")))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "window_start TIMESTAMP(3)",
                                            "window_end TIMESTAMP(3)",
                                            "changes BIGINT")
                                    .addMode(ChangelogMode.insertOnly())
                                    .consumedValues(
                                            "+I[2024-01-01T10:00, 2024-01-01T11:00, 3]",
                                            "+I[2024-01-01T11:00, 2024-01-01T12:00, 1]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "WITH order_changes AS ("
                                    + "SELECT * FROM TO_APPEND(input => TABLE orders)) "
                                    + "SELECT window_start, window_end, COUNT(*) AS changes "
                                    + "FROM TUMBLE(TABLE order_changes, DESCRIPTOR(ts), INTERVAL '1' HOUR) "
                                    + "GROUP BY window_start, window_end")
                    .build();

    public static final TableTestProgram DEDUPLICATE =
            TableTestProgram.of(
                            "to-append-deduplicate",
                            "deduplication is an updating query that only emits inserts")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_table")
                                    .addSchema("id INT", "v STRING")
                                    .addMode(ChangelogMode.insertOnly())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "a"),
                                            Row.ofKind(RowKind.INSERT, 1, "a"))
                                    .build())
                    .setupTableSink(appendOnlySink("+I[1, a]"))
                    .runSql(
                            "INSERT INTO sink "
                                    + "WITH deduplicated AS ("
                                    + "SELECT * FROM source_table "
                                    + "QUALIFY ROW_NUMBER() OVER (PARTITION BY id, v ORDER BY id) = 1) "
                                    + "SELECT * FROM TO_APPEND(input => TABLE deduplicated)")
                    .build();

    public static final TableTestProgram TABLE_API =
            TableTestProgram.of(
                            "to-append-table-api",
                            "Table.toAppend() turns upserts into inserts and drops the delete")
                    .setupTableSource(UPSERT_SOURCE)
                    .setupTableSink(appendOnlySink("+I[1, a]", "+I[1, b]"))
                    .runTableApi(env -> env.from("source_table").toAppend(), "sink")
                    .build();

    private static SinkTestStep appendOnlySink(String... consumedValues) {
        return SinkTestStep.newBuilder("sink")
                .addSchema("id INT", "v STRING")
                .addMode(ChangelogMode.insertOnly())
                .consumedValues(consumedValues)
                .build();
    }
}
