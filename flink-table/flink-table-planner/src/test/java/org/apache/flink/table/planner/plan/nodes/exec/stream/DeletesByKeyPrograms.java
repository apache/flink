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

import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.concurrent.CompletableFuture;

/**
 * Tests for verifying semantic of operations when sources produce deletes by key only and the sink
 * can accept deletes by key only as well.
 */
public final class DeletesByKeyPrograms {

    /**
     * Tests a simple INSERT INTO SELECT scenario where ChangelogNormalize can be eliminated since
     * we don't need UPDATE_BEFORE, and we have key information for all changes.
     */
    public static final TableTestProgram DELETE_BY_KEY_DELETE_BY_KEY =
            TableTestProgram.of(
                            "delete-by-key-delete-by-key",
                            "No ChangelogNormalize: validates results when querying source with deletes by key"
                                    + " only, writing to sink supporting deletes by key only, which"
                                    + " is a case where ChangelogNormalize can be eliminated")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice", 10),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob", 20),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, null, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "Bob", 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption(
                                            "changelog-mode",
                                            "I,UA,D") // Insert, UpdateAfter, Delete
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, Alice, 10]",
                                            "+I[2, Bob, 20]",
                                            "-D[1, null, null]",
                                            "+U[2, Bob, 30]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, name, v FROM source_t")
                    .build();

    public static final TableTestProgram DELETE_BY_KEY_DELETE_BY_KEY_WITH_PROJECTION =
            TableTestProgram.of(
                            "delete-by-key-delete-by-key-with-projection",
                            "No ChangelogNormalize: validates results when querying source with deletes by key"
                                    + " only, writing to sink supporting deletes by key only with a"
                                    + "projection, which is a case where ChangelogNormalize can be"
                                    + " eliminated")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING NOT NULL",
                                            "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice", 10),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob", 20),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, null, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "Bob", 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption(
                                            "changelog-mode",
                                            "I,UA,D") // Insert, UpdateAfter, Delete
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, Alice, 12]",
                                            "+I[2, Bob, 22]",
                                            "-D[1, null, null]",
                                            "+U[2, Bob, 32]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, name, v + 2 FROM source_t")
                    .build();

    public static final TableTestProgram DELETE_BY_KEY_FULL_DELETE =
            TableTestProgram.of(
                            "delete-by-key-full-delete",
                            "ChangelogNormalize: validates results when querying source with deletes by key"
                                    + " only, writing to sink supporting requiring full deletes, "
                                    + "which is a case where ChangelogNormalize stays")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice", 10),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob", 20),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, null, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "Bob", 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "false")
                                    .consumedValues(
                                            "+I[1, Alice, 10]",
                                            "+I[2, Bob, 20]",
                                            "-D[1, Alice, 10]",
                                            "+U[2, Bob, 30]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, name, v FROM source_t")
                    .build();

    public static final TableTestProgram FULL_DELETE_FULL_DELETE =
            TableTestProgram.of(
                            "full-delete-full-delete",
                            "No ChangelogNormalize: validates results when querying source with full deletes, "
                                    + "writing to sink requiring full deletes, which is a case"
                                    + " where ChangelogNormalize can be eliminated")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "false")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice", 10),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob", 20),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, "Alice", 10),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "Bob", 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "false")
                                    .consumedValues(
                                            "+I[1, Alice, 10]",
                                            "+I[2, Bob, 20]",
                                            "-D[1, Alice, 10]",
                                            "+U[2, Bob, 30]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, name, v FROM source_t")
                    .build();

    public static final TableTestProgram JOIN_INTO_FULL_DELETES =
            TableTestProgram.of(
                            "join-into-full-delete",
                            "ChangelogNormalize: validates results when joining sources with deletes by key"
                                    + " only, writing to sink requiring full deletes, which"
                                    + " is a case where ChangelogNormalize stays")
                    .setupTableSource(
                            SourceTestStep.newBuilder("left_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 20),
                                            Row.ofKind(RowKind.INSERT, 3, 30),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 3, 40))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("right_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "name STRING")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.INSERT, 3, "Emily"),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "BOB"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "false")
                                    .testMaterializedData()
                                    .consumedValues("+I[3, Emily, 40]", "+I[2, BOB, 20]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT l.id, r.name, l.v FROM left_t l JOIN right_t r ON l.id = r.id")
                    .build();

    public static final TableTestProgram JOIN_INTO_DELETES_BY_KEY =
            TableTestProgram.of(
                            "join-into-delete-by-key",
                            "No ChangelogNormalize: validates results when joining sources with deletes by key"
                                    + " only, writing to sink supporting deletes by key, which"
                                    + " is a case where ChangelogNormalize can be removed")
                    .setupTableSource(
                            SourceTestStep.newBuilder("left_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 20),
                                            Row.ofKind(RowKind.INSERT, 3, 30),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 3, 40))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("right_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "name STRING")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.INSERT, 3, "Emily"),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "BOB"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "v INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .testMaterializedData()
                                    .consumedValues("+I[2, BOB, 20]", "+I[3, Emily, 40]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT l.id, r.name, l.v FROM left_t l JOIN right_t r ON l.id = r.id")
                    .build();

    public static final TableTestProgram DELETE_BY_KEY_DELETE_BY_KEY_WITH_EXPRESSION =
            TableTestProgram.of(
                            "delete-by-key-delete-by-key-with-expression",
                            "NOT NULL constrains have no effect. The row constructor expression"
                                    + "is not evaluated for partial deletion.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "arr ARRAY<INT> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, new Integer[] {1, 2}),
                                            Row.ofKind(RowKind.INSERT, 2, new Integer[] {3}),
                                            // Delete by key: NOT NULL array column is null
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER, 2, new Integer[] {3, 4}))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b ARRAY<INT>> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, +I[1, [1, 2]]]",
                                            "+I[2, +I[2, [3]]]",
                                            "-D[1, null]",
                                            "+U[2, +I[2, [3, 4]]]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, ROW(id, arr) FROM source_t")
                    .build();

    public static final TableTestProgram DELETE_BY_KEY_DELETE_BY_KEY_WITH_EXPRESSION_AND_FILTER =
            TableTestProgram.of(
                            "delete-by-key-delete-by-key-with-expression-and-filter",
                            "A key-safe filter combined with a non-key row constructor expression."
                                    + " Exercises the local-ref scope isolation between the filter,"
                                    + " the key-only projection and the full projection.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "arr ARRAY<INT> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            // Filtered out by the WHERE clause below
                                            Row.ofKind(RowKind.INSERT, 0, new Integer[] {99}),
                                            Row.ofKind(RowKind.INSERT, 1, new Integer[] {1, 2}),
                                            Row.ofKind(RowKind.INSERT, 2, new Integer[] {3}),
                                            // Delete by key: NOT NULL array column is null
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER, 2, new Integer[] {3, 4}))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id STRING PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b ARRAY<INT>> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, +I[1, [1, 2]]]",
                                            "+I[2, +I[2, [3]]]",
                                            "-D[1, null]",
                                            "+U[2, +I[2, [3, 4]]]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT CAST(id AS STRING), ROW(id, arr) "
                                    + "FROM source_t WHERE CAST(id AS STRING) <> '0'")
                    .build();

    public static final TableTestProgram DELETE_BY_KEY_DELETE_BY_KEY_WITH_DUPLICATE_KEY =
            TableTestProgram.of(
                            "delete-by-key-delete-by-key-with-duplicate-key",
                            "The same key column is projected twice under different names, so the"
                                    + " calc has multiple candidate upsert keys for its output. Both"
                                    + " must be preserved on a partial delete, not just one picked"
                                    + " arbitrarily.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING NOT NULL",
                                            "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice", 10),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob", 20),
                                            // Delete by key
                                            Row.ofKind(RowKind.DELETE, 1, null, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "Bob", 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT",
                                            // Injective cast
                                            "id2 STRING PRIMARY KEY NOT ENFORCED",
                                            "name STRING NOT NULL",
                                            "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, 1, Alice, 12]",
                                            "+I[2, 2, Bob, 22]",
                                            "-D[1, 1, null, null]",
                                            "+U[2, 2, Bob, 32]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT id, CAST(id AS STRING) AS id2, name, v + 2 FROM source_t")
                    .build();

    public static final TableTestProgram DELETE_BY_KEY_ASYNC_CALC_FALLS_BACK_TO_FULL_DELETE =
            TableTestProgram.of(
                            "delete-by-key-async-calc-falls-back-to-full-delete",
                            "An async calc invokes a remote function per row, so the planner is"
                                    + " conservative and never lets a delete-by-key tombstone reach"
                                    + " it: a ChangelogNormalize is kept upstream to materialize"
                                    + " the full row instead, even though the sink itself would"
                                    + " accept delete-by-key. Without this, the delete-by-key"
                                    + " tombstone's null (for a NOT NULL column) would silently"
                                    + " leak through the async calc as a partial delete, instead of"
                                    + " the sink receiving the materialized full row.")
                    .setupTemporaryCatalogFunction("udf1", IncrementAsyncFunction.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED", "v BIGINT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10L),
                                            Row.ofKind(RowKind.INSERT, 2, 20L),
                                            // Delete by key: NOT NULL non-key column is null
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 30L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v2 BIGINT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, 11]",
                                            "+I[2, 21]",
                                            // Full delete: the previous value (10) is materialized
                                            // by the ChangelogNormalize kept upstream of the async
                                            // calc, not the null carried by the source's tombstone.
                                            "-D[1, 11]",
                                            "+U[2, 31]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, udf1(v) FROM source_t")
                    .build();

    /** Increments a {@code BIGINT} input asynchronously. */
    public static class IncrementAsyncFunction extends AsyncScalarFunction {
        public void eval(CompletableFuture<Long> future, Long l) {
            future.complete(l + 1);
        }
    }

    private DeletesByKeyPrograms() {}
}
