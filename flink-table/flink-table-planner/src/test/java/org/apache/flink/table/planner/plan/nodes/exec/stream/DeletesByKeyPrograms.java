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

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Map;

/**
 * Tests for verifying semantic of operations when sources produce deletes by key only and the sink
 * can accept deletes by key only as well.
 */
public final class DeletesByKeyPrograms {

    /**
     * Tests a simple INSERT INTO SELECT scenario where ChangelogNormalize can be eliminated since
     * we don't need UPDATE_BEFORE, and we have key information for all changes.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_DELETE_BY_KEY =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key",
                            "No ChangelogNormalize: validates results when querying source with deletes by key"
                                    + " only, writing to sink supporting deletes by key only, which"
                                    + " is a case where ChangelogNormalize can be eliminated")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "`value` INT")
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
                                            "`value` INT")
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
                    .runSql("INSERT INTO sink_t SELECT id, name, `value` FROM source_t")
                    .build();

    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_DELETE_BY_KEY_WITH_PROJECTION =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-projection",
                            "No ChangelogNormalize: validates results when querying source with deletes by key"
                                    + " only, writing to sink supporting deletes by key only with a"
                                    + "projection, which is a case where ChangelogNormalize can be"
                                    + " eliminated")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING NOT NULL",
                                            "`value` INT NOT NULL")
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
                                            "`value` INT")
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
                    .runSql("INSERT INTO sink_t SELECT id, name, `value` + 2 FROM source_t")
                    .build();

    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_FULL_DELETE =
            TableTestProgram.of(
                            "select-delete-on-key-to-full-delete",
                            "ChangelogNormalize: validates results when querying source with deletes by key"
                                    + " only, writing to sink supporting requiring full deletes, "
                                    + "which is a case where ChangelogNormalize stays")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "`value` INT")
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
                                            "`value` INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "false")
                                    .consumedValues(
                                            "+I[1, Alice, 10]",
                                            "+I[2, Bob, 20]",
                                            "-D[1, Alice, 10]",
                                            "+U[2, Bob, 30]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, name, `value` FROM source_t")
                    .build();

    public static final TableTestProgram INSERT_SELECT_FULL_DELETE_FULL_DELETE =
            TableTestProgram.of(
                            "select-full-delete-to-full-delete",
                            "No ChangelogNormalize: validates results when querying source with full deletes, "
                                    + "writing to sink requiring full deletes, which is a case"
                                    + " where ChangelogNormalize can be eliminated")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "name STRING",
                                            "`value` INT")
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
                                            "`value` INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "false")
                                    .consumedValues(
                                            "+I[1, Alice, 10]",
                                            "+I[2, Bob, 20]",
                                            "-D[1, Alice, 10]",
                                            "+U[2, Bob, 30]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, name, `value` FROM source_t")
                    .build();

    public static final TableTestProgram JOIN_INTO_FULL_DELETES =
            TableTestProgram.of(
                            "join-to-full-delete",
                            "ChangelogNormalize: validates results when joining sources with deletes by key"
                                    + " only, writing to sink requiring full deletes, which"
                                    + " is a case where ChangelogNormalize stays")
                    .setupTableSource(
                            SourceTestStep.newBuilder("left_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "`value` INT")
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
                                            "`value` INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "false")
                                    .testMaterializedData()
                                    .consumedValues("+I[3, Emily, 40]", "+I[2, BOB, 20]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT l.id, r.name, l.`value` FROM left_t l JOIN right_t r ON l.id = r.id")
                    .build();

    public static final TableTestProgram JOIN_INTO_DELETES_BY_KEY =
            TableTestProgram.of(
                            "join-to-delete-on-key",
                            "No ChangelogNormalize: validates results when joining sources with deletes by key"
                                    + " only, writing to sink supporting deletes by key, which"
                                    + " is a case where ChangelogNormalize can be removed")
                    .setupTableSource(
                            SourceTestStep.newBuilder("left_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "`value` INT")
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
                                            "`value` INT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .testMaterializedData()
                                    .consumedValues("+I[2, BOB, 20]", "+I[3, Emily, 40]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT l.id, r.name, l.`value` FROM left_t l JOIN right_t r ON l.id = r.id")
                    .build();

    /**
     * A delete-by-key tombstone carries null for a NOT NULL ARRAY wrapped in a {@code ROW(...)}
     * projection.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_NESTED_NOT_NULL_ARRAY =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-nested-not-null-array",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL ARRAY column wrapped in a ROW(...) projection; validates"
                                    + " that row construction does not fail on the null value")
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
                                            "r ROW<a INT, b ARRAY<INT>>")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, +I[1, [1, 2]]]",
                                            "+I[2, +I[2, [3]]]",
                                            "-D[1, +I[1, null]]",
                                            "+U[2, +I[2, [3, 4]]]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, ROW(id, arr) FROM source_t")
                    .build();

    /**
     * Same as the ARRAY variant but for a NOT NULL {@code MAP} column wrapped in {@code ROW(...)}.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_NESTED_NOT_NULL_MAP =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-nested-not-null-map",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL MAP column wrapped in a ROW(...) projection")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "m MAP<INT, INT> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, Map.of(1, 10)),
                                            Row.ofKind(RowKind.INSERT, 2, Map.of(2, 20)),
                                            // Delete by key: NOT NULL map column is null
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, Map.of(2, 30)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b MAP<INT, INT>>")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, +I[1, {1=10}]]",
                                            "+I[2, +I[2, {2=20}]]",
                                            "-D[1, +I[1, null]]",
                                            "+U[2, +I[2, {2=30}]]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, ROW(id, m) FROM source_t")
                    .build();

    /**
     * Same as the ARRAY variant but for a NOT NULL {@code ROW} column wrapped in {@code ROW(...)}.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_NESTED_NOT_NULL_ROW =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-nested-not-null-row",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL ROW column wrapped in a ROW(...) projection")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "nested ROW<x INT, y INT> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, Row.of(1, 10)),
                                            Row.ofKind(RowKind.INSERT, 2, Row.of(2, 20)),
                                            // Delete by key: NOT NULL row column is null
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, Row.of(2, 30)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b ROW<x INT, y INT>>")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, +I[1, +I[1, 10]]]",
                                            "+I[2, +I[2, +I[2, 20]]]",
                                            "-D[1, +I[1, null]]",
                                            "+U[2, +I[2, +I[2, 30]]]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, ROW(id, nested) FROM source_t")
                    .build();

    /** Same as the ARRAY variant but for a NOT NULL {@code ARRAY<ROW>} column. */
    public static final TableTestProgram
            INSERT_SELECT_DELETE_BY_KEY_WITH_NESTED_NOT_NULL_ARRAY_OF_ROW =
                    TableTestProgram.of(
                                    "select-delete-on-key-to-delete-on-key-with-nested-not-null-array-of-row",
                                    "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                            + " NULL ARRAY<ROW> column wrapped in a ROW(...) projection")
                            .setupTableSource(
                                    SourceTestStep.newBuilder("source_t")
                                            .addSchema(
                                                    "id INT PRIMARY KEY NOT ENFORCED",
                                                    "arr ARRAY<ROW<x INT, y INT>> NOT NULL")
                                            .addOption("changelog-mode", "I,UA,D")
                                            .addOption("source.produces-delete-by-key", "true")
                                            .producedValues(
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            1,
                                                            new Row[] {Row.of(1, 10)}),
                                                    Row.ofKind(
                                                            RowKind.INSERT,
                                                            2,
                                                            new Row[] {Row.of(2, 20)}),
                                                    // Delete by key: NOT NULL array column is null
                                                    Row.ofKind(RowKind.DELETE, 1, null),
                                                    // Update after only
                                                    Row.ofKind(
                                                            RowKind.UPDATE_AFTER,
                                                            2,
                                                            new Row[] {Row.of(2, 30)}))
                                            .build())
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "id INT PRIMARY KEY NOT ENFORCED",
                                                    "r ROW<a INT, b ARRAY<ROW<x INT, y INT>>>")
                                            .addOption("changelog-mode", "I,UA,D")
                                            .addOption("sink.supports-delete-by-key", "true")
                                            .consumedValues(
                                                    "+I[1, +I[1, [+I[1, 10]]]]",
                                                    "+I[2, +I[2, [+I[2, 20]]]]",
                                                    "-D[1, +I[1, null]]",
                                                    "+U[2, +I[2, [+I[2, 30]]]]")
                                            .build())
                            .runSql("INSERT INTO sink_t SELECT id, ROW(id, arr) FROM source_t")
                            .build();

    /**
     * Same shape as the ARRAY variant but for a NOT NULL {@code STRING} column. The reference-typed
     * String path does not fail today, but the case is kept for coverage.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_NESTED_NOT_NULL_STRING =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-nested-not-null-string",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL STRING column wrapped in a ROW(...) projection")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED", "s STRING NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "a"),
                                            Row.ofKind(RowKind.INSERT, 2, "b"),
                                            // Delete by key: NOT NULL string column is null
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            // Update after only
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "c"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b STRING>")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, +I[1, a]]",
                                            "+I[2, +I[2, b]]",
                                            "-D[1, +I[1, null]]",
                                            "+U[2, +I[2, c]]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, ROW(id, s) FROM source_t")
                    .build();

    /**
     * Same shape as the ARRAY variant but for a NOT NULL primitive {@code INT} column. The
     * primitive path is already guarded; kept for coverage.
     */
    public static final TableTestProgram
            INSERT_SELECT_DELETE_BY_KEY_WITH_NESTED_NOT_NULL_PRIMITIVE =
                    TableTestProgram.of(
                                    "select-delete-on-key-to-delete-on-key-with-nested-not-null-primitive",
                                    "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                            + " NULL INT column wrapped in a ROW(...) projection")
                            .setupTableSource(
                                    SourceTestStep.newBuilder("source_t")
                                            .addSchema(
                                                    "id INT PRIMARY KEY NOT ENFORCED",
                                                    "v INT NOT NULL")
                                            .addOption("changelog-mode", "I,UA,D")
                                            .addOption("source.produces-delete-by-key", "true")
                                            .producedValues(
                                                    Row.ofKind(RowKind.INSERT, 1, 10),
                                                    Row.ofKind(RowKind.INSERT, 2, 20),
                                                    Row.ofKind(RowKind.DELETE, 1, null),
                                                    Row.ofKind(RowKind.UPDATE_AFTER, 2, 30))
                                            .build())
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "id INT PRIMARY KEY NOT ENFORCED",
                                                    "r ROW<a INT, b INT>")
                                            .addOption("changelog-mode", "I,UA,D")
                                            .addOption("sink.supports-delete-by-key", "true")
                                            .consumedValues(
                                                    "+I[1, +I[1, 10]]",
                                                    "+I[2, +I[2, 20]]",
                                                    "-D[1, +I[1, null]]",
                                                    "+U[2, +I[2, 30]]")
                                            .build())
                            .runSql("INSERT INTO sink_t SELECT id, ROW(id, v) FROM source_t")
                            .build();

    /**
     * A LEFT JOIN whose probe (left) side produces a delete-by-key tombstone carrying null for a
     * NOT NULL ARRAY column that is wrapped in a ROW(...) projection.
     */
    public static final TableTestProgram JOIN_DELETE_BY_KEY_WITH_NESTED_NOT_NULL_ARRAY =
            TableTestProgram.of(
                            "join-delete-on-key-with-nested-not-null-array",
                            "No ChangelogNormalize: probe-side delete-by-key tombstone carries null"
                                    + " for a NOT NULL ARRAY column wrapped in a ROW(...) projection"
                                    + " across a join")
                    .setupTableSource(
                            SourceTestStep.newBuilder("left_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "arr ARRAY<INT> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, new Integer[] {1, 2}),
                                            Row.ofKind(RowKind.INSERT, 2, new Integer[] {3}),
                                            Row.ofKind(RowKind.INSERT, 3, new Integer[] {5}),
                                            // Delete by key: NOT NULL array column is null
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 3, new Integer[] {6}))
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
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "BOB"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b ARRAY<INT>>",
                                            "name STRING")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .testMaterializedData()
                                    .consumedValues(
                                            "+I[2, +I[2, [3]], BOB]", "+I[3, +I[3, [6]], Emily]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT l.id, ROW(l.id, l.arr), r.name"
                                    + " FROM left_t l JOIN right_t r ON l.id = r.id")
                    .build();

    /**
     * A delete-by-key tombstone carries null for a NOT NULL {@code INT} column that is used as an
     * element of an {@code ARRAY[...]} literal (element type {@code INT NOT NULL}) wrapped in a
     * {@code ROW(...)} projection. Validates that array construction sets the element to null
     * instead of writing the primitive default.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_NOT_NULL_ARRAY_LITERAL =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-not-null-array-literal",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL INT column used as an element of an ARRAY[...] literal")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 20),
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b ARRAY<INT>>")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, +I[1, [10, 99]]]",
                                            "+I[2, +I[2, [20, 99]]]",
                                            "-D[1, +I[1, [null, 99]]]",
                                            "+U[2, +I[2, [30, 99]]]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, ROW(id, ARRAY[v, 99]) FROM source_t")
                    .build();

    /**
     * Same as the ARRAY literal variant but for a {@code MAP[...]} literal whose value comes from a
     * NOT NULL {@code INT} column (value type {@code INT NOT NULL}).
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_NOT_NULL_MAP_LITERAL =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-not-null-map-literal",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL INT column used as a value of a MAP[...] literal")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 20),
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b MAP<INT, INT>>")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, +I[1, {99=10}]]",
                                            "+I[2, +I[2, {99=20}]]",
                                            "-D[1, +I[1, {99=null}]]",
                                            "+U[2, +I[2, {99=30}]]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, ROW(id, MAP[99, v]) FROM source_t")
                    .build();

    /**
     * A delete-by-key tombstone carries null for a NOT NULL {@code INT} column that is nested in a
     * {@code ROW(...)} serialized by {@code JSON_OBJECT}. The nested field is NOT NULL, so the JSON
     * row converter must still guard on the runtime null and emit {@code null} instead of reading
     * the primitive default.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_JSON_OBJECT_NESTED_ROW =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-json-object-nested-row",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL INT column nested in a ROW serialized by JSON_OBJECT")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 20),
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "j STRING")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, {\"r\":{\"EXPR$0\":10}}]",
                                            "+I[2, {\"r\":{\"EXPR$0\":20}}]",
                                            "-D[1, {\"r\":{\"EXPR$0\":null}}]",
                                            "+U[2, {\"r\":{\"EXPR$0\":30}}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT id, JSON_OBJECT('r' VALUE ROW(v)) FROM source_t")
                    .build();

    /**
     * Same as the JSON_OBJECT nested ROW variant but for a NOT NULL {@code INT} element nested in
     * an {@code ARRAY[...]} serialized by {@code JSON_OBJECT}.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_JSON_OBJECT_NESTED_ARRAY =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-json-object-nested-array",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL INT element nested in an ARRAY serialized by JSON_OBJECT")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 20),
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "j STRING")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, {\"a\":[10,99]}]",
                                            "+I[2, {\"a\":[20,99]}]",
                                            "-D[1, {\"a\":[null,99]}]",
                                            "+U[2, {\"a\":[30,99]}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT id, JSON_OBJECT('a' VALUE ARRAY[v, 99]) FROM source_t")
                    .build();

    /**
     * Same as the JSON_OBJECT nested ROW variant but for a NOT NULL {@code INT} value nested in a
     * {@code MAP[...]} serialized by {@code JSON_OBJECT}.
     */
    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_JSON_OBJECT_NESTED_MAP =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-json-object-nested-map",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL INT value nested in a MAP serialized by JSON_OBJECT")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 20),
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "j STRING")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, {\"m\":{\"k\":10}}]",
                                            "+I[2, {\"m\":{\"k\":20}}]",
                                            "-D[1, {\"m\":{\"k\":null}}]",
                                            "+U[2, {\"m\":{\"k\":30}}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT id, JSON_OBJECT('m' VALUE MAP['k', v]) FROM source_t")
                    .build();

    public static final TableTestProgram INSERT_SELECT_DELETE_BY_KEY_WITH_NOT_NULL_CAST =
            TableTestProgram.of(
                            "select-delete-on-key-to-delete-on-key-with-not-null-cast",
                            "No ChangelogNormalize: a delete-by-key tombstone carries null for a NOT"
                                    + " NULL INT column that is CAST to BIGINT")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v INT NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10),
                                            Row.ofKind(RowKind.INSERT, 2, 20),
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 30))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id INT PRIMARY KEY NOT ENFORCED", "v BIGINT")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedValues(
                                            "+I[1, 10]", "+I[2, 20]", "-D[1, null]", "+U[2, 30]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, CAST(v AS BIGINT) FROM source_t")
                    .build();

    private DeletesByKeyPrograms() {}
}
