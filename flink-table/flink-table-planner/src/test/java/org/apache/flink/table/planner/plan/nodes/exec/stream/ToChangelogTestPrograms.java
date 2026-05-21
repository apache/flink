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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;

/** {@link TableTestProgram} definitions for testing the built-in TO_CHANGELOG PTF. */
public class ToChangelogTestPrograms {

    private static final SourceTestStep SIMPLE_SOURCE =
            SourceTestStep.newBuilder("t")
                    .addSchema("id INT", "name STRING")
                    .addMode(ChangelogMode.insertOnly())
                    .producedValues(Row.ofKind(RowKind.INSERT, 1, "Alice"))
                    .build();

    // --------------------------------------------------------------------------------------------
    // SQL tests
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram INSERT =
            TableTestProgram.of("to-changelog-insert-only", "insert-only input produces op=INSERT")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("id INT", "name STRING")
                                    .addMode(ChangelogMode.insertOnly())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("op STRING", "id INT", "name STRING")
                                    .consumedValues("+I[INSERT, 1, Alice]", "+I[INSERT, 2, Bob]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM TO_CHANGELOG(input => TABLE t)")
                    .build();

    public static final TableTestProgram RETRACT =
            TableTestProgram.of(
                            "to-changelog-updating-input",
                            "retract input produces all op codes including UPDATE_BEFORE")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            Row.ofKind(RowKind.DELETE, "Bob", 20L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("op STRING", "name STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[INSERT, Alice, 10]",
                                            "+I[INSERT, Bob, 20]",
                                            "+I[UPDATE_BEFORE, Alice, 10]",
                                            "+I[UPDATE_AFTER, Alice, 30]",
                                            "+I[DELETE, Bob, 20]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM TO_CHANGELOG(input => TABLE t)")
                    .build();

    /** Retract input through TO_CHANGELOG, split across a compiled-plan + savepoint restore. */
    public static final TableTestProgram RETRACT_RESTORE =
            TableTestProgram.of(
                            "to-changelog-retract-restore",
                            "TO_CHANGELOG over a retract source restores via compiled plan + "
                                    + "savepoint")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            Row.ofKind(RowKind.DELETE, "Bob", 20L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("op STRING", "name STRING", "score BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[INSERT, Alice, 10]", "+I[INSERT, Bob, 20]")
                                    .consumedAfterRestore(
                                            "+I[UPDATE_BEFORE, Alice, 10]",
                                            "+I[UPDATE_AFTER, Alice, 30]",
                                            "+I[DELETE, Bob, 20]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM TO_CHANGELOG(input => TABLE t)")
                    .build();

    /** Partitions by a non-leading column ({@code id}, the middle column of three). */
    public static final TableTestProgram RETRACT_PARTITION_BY =
            TableTestProgram.of(
                            "to-changelog-retract-partition-by-middle-column",
                            "PARTITION BY a non-leading column drops it from the function output "
                                    + "without disturbing the order of the remaining columns")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED",
                                            "id STRING",
                                            "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", "EU", 10L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", "EU", 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", "EU", 30L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "id STRING", "op STRING", "name STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[EU, INSERT, Alice, 10]",
                                            "+I[EU, UPDATE_BEFORE, Alice, 10]",
                                            "+I[EU, UPDATE_AFTER, Alice, 30]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG(input => TABLE t PARTITION BY id)")
                    .build();

    public static final TableTestProgram UPSERT =
            TableTestProgram.of(
                            "to-changelog-upsert-input",
                            "upsert input in row semantics gets ChangelogNormalize for UPDATE_BEFORE and emits full deletes")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.upsert())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            // Key-only delete: ChangelogNormalize fills
                                            // in the full row from state
                                            Row.ofKind(RowKind.DELETE, "Bob", null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("op STRING", "name STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[INSERT, Alice, 10]",
                                            "+I[INSERT, Bob, 20]",
                                            "+I[UPDATE_BEFORE, Alice, 10]",
                                            "+I[UPDATE_AFTER, Alice, 30]",
                                            "+I[DELETE, Bob, 20]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM TO_CHANGELOG(input => TABLE t)")
                    .build();

    public static final TableTestProgram UPSERT_PARTITION_BY =
            TableTestProgram.of(
                            "to-changelog-upsert-partition-by",
                            "PARTITION BY upsert key + mapping without UB skips ChangelogNormalize; "
                                    + "default produces_full_deletes=false nulls non-key columns on DELETE")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.upsert())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            Row.ofKind(RowKind.DELETE, "Bob", 20L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("name STRING", "op STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[Alice, C, 10]",
                                            "+I[Bob, C, 20]",
                                            "+I[Alice, C, 30]",
                                            "+I[Bob, D, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t PARTITION BY name, "
                                    + "op => DESCRIPTOR(op), "
                                    + "op_mapping => MAP['INSERT,UPDATE_AFTER', 'C', 'DELETE', 'D'])")
                    .build();

    public static final TableTestProgram UPSERT_PARTITION_BY_KEY_ONLY_DELETES =
            TableTestProgram.of(
                            "to-changelog-upsert-partition-by-key-only-deletes",
                            "PARTITION BY upsert key + mapping without UB/DELETE handles key-only deletes")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.upsert(true))
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            // Key-only delete: source only knows the key.
                                            Row.ofKind(RowKind.DELETE, "Bob", null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("name STRING", "op STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[Alice, U, 10]",
                                            "+I[Bob, U, 20]",
                                            "+I[Alice, U, 30]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t PARTITION BY name, "
                                    + "op => DESCRIPTOR(op), "
                                    + "op_mapping => MAP['INSERT,UPDATE_AFTER', 'U'])")
                    .build();

    public static final TableTestProgram CUSTOM_OP_MAPPING =
            TableTestProgram.of(
                            "to-changelog-custom-op-mapping",
                            "custom op_mapping maps change operations to user-defined codes and drops unmapped")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            Row.ofKind(RowKind.DELETE, "Bob", 20L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("op_code STRING", "name STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[I, Alice, 10]",
                                            "+I[I, Bob, 20]",
                                            "+I[U, Alice, 30]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "op => DESCRIPTOR(op_code), "
                                    + "op_mapping => MAP['INSERT','I', 'UPDATE_AFTER','U'])")
                    .build();

    public static final TableTestProgram CUSTOM_OP_NAME =
            TableTestProgram.of(
                            "to-changelog-custom-op-name", "custom op column name via DESCRIPTOR")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("id INT", "name STRING")
                                    .addMode(ChangelogMode.insertOnly())
                                    .producedValues(Row.ofKind(RowKind.INSERT, 1, "Alice"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("operation STRING", "id INT", "name STRING")
                                    .consumedValues("+I[INSERT, 1, Alice]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "op => DESCRIPTOR(operation))")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Table API test
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram TABLE_API_DEFAULT =
            TableTestProgram.of(
                            "to-changelog-table-api-default",
                            "Table.toChangelog() convenience method")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("id INT", "name STRING")
                                    .addMode(ChangelogMode.insertOnly())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("op STRING", "id INT", "name STRING")
                                    .consumedValues("+I[INSERT, 1, Alice]", "+I[INSERT, 2, Bob]")
                                    .build())
                    .runTableApi(env -> env.from("t").toChangelog(), "sink")
                    .build();

    public static final TableTestProgram TABLE_API_RETRACT_PARTITION_BY =
            TableTestProgram.of(
                            "to-changelog-table-api-retract-partition-by",
                            "PartitionedTable.toChangelog() convenience method")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED",
                                            "id STRING",
                                            "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", "EU", 10L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", "EU", 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", "EU", 30L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "id STRING", "op STRING", "name STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[EU, INSERT, Alice, 10]",
                                            "+I[EU, UPDATE_BEFORE, Alice, 10]",
                                            "+I[EU, UPDATE_AFTER, Alice, 30]")
                                    .build())
                    .runTableApi(env -> env.from("t").partitionBy($("id")).toChangelog(), "sink")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Use case: LAG on updating streams via TO_CHANGELOG
    // --------------------------------------------------------------------------------------------

    /**
     * An upsert source produces INSERT, UPDATE_AFTER, and DELETE events. TO_CHANGELOG converts them
     * to append-only rows with explicit op codes, enabling LAG to track status transitions -
     * something that fails directly on upsert streams.
     */
    public static final TableTestProgram LAG_ON_UPSERT_VIA_CHANGELOG =
            TableTestProgram.of(
                            "to-changelog-lag-on-upsert",
                            "enables LAG on upsert stream with INSERT, UPDATE_AFTER, and DELETE")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(
                                            "order_id INT NOT NULL",
                                            "status STRING",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "PRIMARY KEY (order_id) NOT ENFORCED",
                                            "WATERMARK FOR ts AS ts - INTERVAL '1' SECOND")
                                    .addMode(ChangelogMode.upsert())
                                    .producedValues(
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    1,
                                                    "CREATED",
                                                    Instant.ofEpochMilli(1000)),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    2,
                                                    "CREATED",
                                                    Instant.ofEpochMilli(2000)),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    1,
                                                    "SHIPPED",
                                                    Instant.ofEpochMilli(3000)),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    1,
                                                    "DELIVERED",
                                                    Instant.ofEpochMilli(4000)),
                                            Row.ofKind(
                                                    RowKind.DELETE,
                                                    2,
                                                    "CREATED",
                                                    Instant.ofEpochMilli(5000)))
                                    .build())
                    .setupSql(
                            "CREATE VIEW orders_changelog AS "
                                    + "SELECT op, order_id, status, ts FROM TO_CHANGELOG("
                                    + "  input => TABLE orders, "
                                    + "  op_mapping => MAP['INSERT', 'INSERT', 'UPDATE_AFTER', 'UPDATE_AFTER'])")
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "order_id INT",
                                            "op STRING",
                                            "cur_status STRING",
                                            "prev_status STRING")
                                    .consumedValues(
                                            "+I[1, INSERT, CREATED, null]",
                                            "+I[2, INSERT, CREATED, null]",
                                            "+I[1, UPDATE_AFTER, SHIPPED, CREATED]",
                                            "+I[1, UPDATE_AFTER, DELIVERED, SHIPPED]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT order_id, op, status, "
                                    + "  LAG(status) OVER (PARTITION BY order_id ORDER BY ts) AS prev_status "
                                    + "FROM orders_changelog")
                    .build();

    /**
     * A retract source produces UPDATE_BEFORE + UPDATE_AFTER pairs and DELETE events. TO_CHANGELOG
     * drops UPDATE_BEFORE and DELETE via op_mapping, keeping only forward-looking transitions for
     * LAG to track.
     */
    public static final TableTestProgram LAG_ON_RETRACT_VIA_CHANGELOG =
            TableTestProgram.of(
                            "to-changelog-lag-on-retract",
                            "enables LAG on retract stream, dropping UB and DELETE via op_mapping")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(
                                            "order_id INT NOT NULL",
                                            "status STRING",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "PRIMARY KEY (order_id) NOT ENFORCED",
                                            "WATERMARK FOR ts AS ts - INTERVAL '1' SECOND")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    1,
                                                    "CREATED",
                                                    Instant.ofEpochMilli(1000)),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    2,
                                                    "CREATED",
                                                    Instant.ofEpochMilli(2000)),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE,
                                                    1,
                                                    "CREATED",
                                                    Instant.ofEpochMilli(3000)),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    1,
                                                    "SHIPPED",
                                                    Instant.ofEpochMilli(3000)),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE,
                                                    1,
                                                    "SHIPPED",
                                                    Instant.ofEpochMilli(4000)),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    1,
                                                    "DELIVERED",
                                                    Instant.ofEpochMilli(4000)),
                                            Row.ofKind(
                                                    RowKind.DELETE,
                                                    2,
                                                    "CREATED",
                                                    Instant.ofEpochMilli(5000)))
                                    .build())
                    .setupSql(
                            "CREATE VIEW orders_changelog AS "
                                    + "SELECT op, order_id, status, ts FROM TO_CHANGELOG("
                                    + "  input => TABLE orders, "
                                    + "  op_mapping => MAP['INSERT', 'INSERT', 'UPDATE_AFTER', 'UPDATE_AFTER'])")
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "order_id INT",
                                            "op STRING",
                                            "cur_status STRING",
                                            "prev_status STRING")
                                    .consumedValues(
                                            "+I[1, INSERT, CREATED, null]",
                                            "+I[2, INSERT, CREATED, null]",
                                            "+I[1, UPDATE_AFTER, SHIPPED, CREATED]",
                                            "+I[1, UPDATE_AFTER, DELIVERED, SHIPPED]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "SELECT order_id, op, status, "
                                    + "  LAG(status) OVER (PARTITION BY order_id ORDER BY ts) AS prev_status "
                                    + "FROM orders_changelog")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Use case: deletion flag pattern (comma-separated change operation keys)
    // --------------------------------------------------------------------------------------------

    /**
     * Kafka Connect style deletion flag: INSERT and UPDATE_AFTER both produce deleted='false' and
     * DELETE produces deleted='true'. UPDATE_BEFORE is silently dropped.
     */
    public static final TableTestProgram DELETION_FLAG =
            TableTestProgram.of(
                            "to-changelog-deletion-flag",
                            "comma-separated change operations produce deletion flag output")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            Row.ofKind(RowKind.DELETE, "Bob", 20L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("deleted STRING", "name STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[false, Alice, 10]",
                                            "+I[false, Bob, 20]",
                                            "+I[false, Alice, 30]",
                                            "+I[true, Bob, 20]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "op => DESCRIPTOR(deleted), "
                                    + "op_mapping => MAP['INSERT, UPDATE_AFTER', 'false', 'DELETE', 'true'])")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Error validation tests
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram INVALID_DESCRIPTOR =
            TableTestProgram.of(
                            "to-changelog-invalid-descriptor",
                            "fails when DESCRIPTOR has multiple columns")
                    .setupTableSource(SIMPLE_SOURCE)
                    .runFailingSql(
                            "SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "op => DESCRIPTOR(a, b))",
                            ValidationException.class,
                            "The descriptor for argument 'op' must contain exactly one column name.")
                    .build();

    public static final TableTestProgram INVALID_OP_MAPPING =
            TableTestProgram.of(
                            "to-changelog-invalid-op-mapping",
                            "fails when op_mapping has invalid change operation name")
                    .setupTableSource(SIMPLE_SOURCE)
                    .runFailingSql(
                            "SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "op_mapping => MAP['INVALID_KIND', 'X'])",
                            ValidationException.class,
                            "Unknown change operation: 'INVALID_KIND'")
                    .build();

    public static final TableTestProgram OP_MAPPING_REFERENCES_UNSUPPORTED_KIND =
            TableTestProgram.of(
                            "to-changelog-op-mapping-references-unsupported-kind",
                            "fails when op_mapping references a change operation the input cannot produce")
                    .setupTableSource(SIMPLE_SOURCE)
                    .runFailingSql(
                            "SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "op_mapping => MAP['INSERT', 'I', 'DELETE', 'D'])",
                            ValidationException.class,
                            "the input table only produces [INSERT] and does not produce [DELETE]")
                    .build();

    public static final TableTestProgram DUPLICATE_ROW_KIND =
            TableTestProgram.of(
                            "to-changelog-duplicate-rowkind",
                            "fails when a change operation appears in multiple op_mapping entries")
                    .setupTableSource(SIMPLE_SOURCE)
                    .runFailingSql(
                            "SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "op_mapping => MAP['INSERT, DELETE', 'A', 'DELETE', 'B'])",
                            ValidationException.class,
                            "Duplicate change operation: 'DELETE'")
                    .build();

    public static final TableTestProgram PRODUCES_FULL_DELETES_ON_APPEND_ONLY_INPUT =
            TableTestProgram.of(
                            "to-changelog-produces-full-deletes-on-append-only-input",
                            "fails when produces_full_deletes=true on an input that never emits DELETE rows")
                    .setupTableSource(SIMPLE_SOURCE)
                    .runFailingSql(
                            "SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "produces_full_deletes => true)",
                            ValidationException.class,
                            "the input table only produces [INSERT] and never emits DELETE rows")
                    .build();

    public static final TableTestProgram PRODUCES_FULL_DELETES_WITHOUT_DELETE_IN_OP_MAPPING =
            TableTestProgram.of(
                            "to-changelog-produces-full-deletes-without-delete-in-op-mapping",
                            "fails when produces_full_deletes=true but the active op_mapping strips DELETE")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(Row.ofKind(RowKind.INSERT, "Alice", 10L))
                                    .build())
                    .runFailingSql(
                            "SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "op_mapping => MAP['INSERT, UPDATE_AFTER', 'X'], "
                                    + "produces_full_deletes => true)",
                            ValidationException.class,
                            "the active 'op_mapping' does not map DELETE rows")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Row semantics x delete handling matrix
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram ROW_SEM_PARTIAL_DELETES =
            TableTestProgram.of(
                            "to-changelog-row-sem-partial-deletes",
                            "row semantics: produces_full_deletes=false skips ChangelogNormalize and a partial DELETE row from the input passes through unchanged")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            Row.ofKind(RowKind.DELETE, "Bob", null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("op STRING", "name STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[INSERT, Alice, 10]",
                                            "+I[INSERT, Bob, 20]",
                                            "+I[UPDATE_BEFORE, Alice, 10]",
                                            "+I[UPDATE_AFTER, Alice, 30]",
                                            "+I[DELETE, Bob, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "produces_full_deletes => false)")
                    .build();

    public static final TableTestProgram ROW_SEM_FORCE_FULL_DELETES =
            TableTestProgram.of(
                            "to-changelog-row-sem-force-full-deletes",
                            "row semantics: produces_full_deletes=true forces ChangelogNormalize to materialize the full DELETE row from an upsert source emitting key-only deletes")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.upsert())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            // Key-only delete: ChangelogNormalize fills the row.
                                            Row.ofKind(RowKind.DELETE, "Alice", null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("op STRING", "name STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[INSERT, Alice, 10]", "+I[DELETE, Alice, 10]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t, "
                                    + "produces_full_deletes => true)")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Set semantics x delete handling matrix
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram SET_SEM_FORCE_PARTIAL_DELETES =
            TableTestProgram.of(
                            "to-changelog-set-sem-force-partial-deletes",
                            "set semantics: produces_full_deletes=false nulls non-partition-key columns on DELETE even when the input row is fully populated")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            Row.ofKind(RowKind.DELETE, "Bob", 20L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("name STRING", "op STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[Alice, INSERT, 10]",
                                            "+I[Bob, INSERT, 20]",
                                            "+I[Alice, UPDATE_AFTER, 30]",
                                            "+I[Bob, DELETE, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t PARTITION BY name,"
                                    + "produces_full_deletes => false)")
                    .build();

    public static final TableTestProgram SET_SEM_PARTIAL_DELETES =
            TableTestProgram.of(
                            "to-changelog-set-sem-partial-deletes",
                            "set semantics: produces_full_deletes=false (default) lets a partial DELETE row from the input pass through with non-partition-key columns null")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.DELETE, "Alice", null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("name STRING", "op STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[Alice, INSERT, 10]", "+I[Alice, DELETE, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t PARTITION BY name)")
                    .build();

    public static final TableTestProgram SET_SEM_FULL_DELETES =
            TableTestProgram.of(
                            "to-changelog-set-sem-full-deletes",
                            "set semantics: produces_full_deletes=true on an input that already emits full deletes is a no-op for the planner and the full DELETE row reaches the output")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.DELETE, "Alice", 10L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("name STRING", "op STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[Alice, INSERT, 10]", "+I[Alice, DELETE, 10]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t PARTITION BY name, "
                                    + "produces_full_deletes => true)")
                    .build();

    public static final TableTestProgram SET_SEM_FORCE_FULL_DELETES =
            TableTestProgram.of(
                            "to-changelog-set-sem-force-full-deletes",
                            "set semantics: produces_full_deletes=true forces ChangelogNormalize to materialize the full DELETE row from an upsert source emitting key-only deletes")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addMode(ChangelogMode.upsert())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10L),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 30L),
                                            Row.ofKind(RowKind.DELETE, "Bob", null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("name STRING", "op STRING", "score BIGINT")
                                    .consumedValues(
                                            "+I[Alice, INSERT, 10]",
                                            "+I[Bob, INSERT, 20]",
                                            "+I[Alice, UPDATE_BEFORE, 10]",
                                            "+I[Alice, UPDATE_AFTER, 30]",
                                            "+I[Bob, DELETE, 20]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM TO_CHANGELOG("
                                    + "input => TABLE t PARTITION BY name, "
                                    + "produces_full_deletes => true)")
                    .build();
}
