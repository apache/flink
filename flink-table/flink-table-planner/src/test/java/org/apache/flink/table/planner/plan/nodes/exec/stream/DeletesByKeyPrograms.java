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
                                            "-D[1, , -1]",
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
                                    .consumedValues(
                                            "+I[1, Alice, 10]",
                                            "+I[2, Bob, 20]",
                                            "+I[3, Emily, 30]",
                                            "-D[1, Alice, 10]",
                                            "+U[3, Emily, 40]",
                                            "+U[2, BOB, 20]")
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
                                    .consumedValues(
                                            "+I[1, Alice, 10]",
                                            "+I[2, Bob, 20]",
                                            "+I[3, Emily, 30]",
                                            "-D[1, Alice, null]",
                                            "+U[3, Emily, 40]",
                                            "+U[2, BOB, 20]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT l.id, r.name, l.`value` FROM left_t l JOIN right_t r ON l.id = r.id")
                    .build();

    private DeletesByKeyPrograms() {}
}
