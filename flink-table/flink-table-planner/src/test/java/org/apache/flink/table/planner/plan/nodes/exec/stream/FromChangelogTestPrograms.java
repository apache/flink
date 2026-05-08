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

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.api.Expressions.$;

/** {@link TableTestProgram} definitions for testing the built-in FROM_CHANGELOG PTF. */
public class FromChangelogTestPrograms {

    private static final String[] SIMPLE_CDC_SCHEMA = {"id INT", "op STRING", "name STRING"};

    // --------------------------------------------------------------------------------------------
    // SQL tests
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram RETRACT =
            TableTestProgram.of("from-changelog-retract", "retract changelog with default mapping")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedValues(
                                            Row.of(1, "INSERT", "Alice"),
                                            Row.of(2, "INSERT", "Bob"),
                                            Row.of(1, "UPDATE_BEFORE", "Alice"),
                                            Row.of(1, "UPDATE_AFTER", "Alice2"),
                                            Row.of(2, "DELETE", "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "Alice2"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream)")
                    .build();

    public static final TableTestProgram CUSTOM_OP_MAPPING =
            TableTestProgram.of(
                            "from-changelog-custom-op-mapping",
                            "custom op_mapping with comma-separated keys")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedValues(
                                            Row.of(1, "c", "Alice"),
                                            Row.of(2, "r", "Bob"),
                                            Row.of(1, "ub", "Alice"),
                                            Row.of(1, "ua", "Alice2"),
                                            Row.of(2, "d", "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "Alice2"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream, "
                                    + "op_mapping => MAP['c, r', 'INSERT', 'ub', 'UPDATE_BEFORE', 'ua', 'UPDATE_AFTER', 'd', 'DELETE'])")
                    .build();

    public static final TableTestProgram SKIP_INVALID_OP_HANDLING =
            TableTestProgram.of(
                            "from-changelog-unmapped-codes-dropped",
                            "unmapped op codes are silently dropped when configured")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedValues(
                                            Row.of(1, "INSERT", "Alice"),
                                            Row.of(2, "INSERT", "Bob"),
                                            Row.of(1, "UNKNOWN", "Alice2"),
                                            Row.of(2, "DELETE", "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream,"
                                    + "error_handling => 'SKIP')")
                    .build();

    public static final TableTestProgram SKIP_NULL_OP_CODE =
            TableTestProgram.of(
                            "from-changelog-null-op-code-dropped",
                            "NULL op codes are silently dropped when configured")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedValues(
                                            Row.of(1, "INSERT", "Alice"),
                                            Row.of(2, null, "Bob"),
                                            Row.of(3, "INSERT", "Carol"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 3, "Carol"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream,"
                                    + "error_handling => 'SKIP')")
                    .build();

    public static final TableTestProgram CUSTOM_OP_NAME =
            TableTestProgram.of(
                            "from-changelog-custom-op-name", "custom op column name via DESCRIPTOR")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema("id INT", "operation STRING", "name STRING")
                                    .producedValues(
                                            Row.of(1, "INSERT", "Alice"),
                                            Row.of(1, "UPDATE_BEFORE", "Alice"),
                                            Row.of(1, "UPDATE_AFTER", "Alice2"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "Alice2"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream, "
                                    + "op => DESCRIPTOR(operation))")
                    .build();

    public static final TableTestProgram RETRACT_PARTITION_BY =
            TableTestProgram.of(
                            "from-changelog-retract-partition-by",
                            "retract changelog with PARTITION BY")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema("name STRING", "id INT", "op STRING")
                                    .producedValues(
                                            Row.of("Alice", 1, "INSERT"),
                                            Row.of("Bob", 2, "INSERT"),
                                            Row.of("Alice", 1, "UPDATE_BEFORE"),
                                            Row.of("Alice2", 1, "UPDATE_AFTER"),
                                            Row.of("Bob", 2, "DELETE"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "Alice2"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream PARTITION BY id)")
                    .build();

    public static final TableTestProgram DELETION_FLAG_PARTITION_BY =
            TableTestProgram.of(
                            "from-changelog-deletion-flag-partition-by",
                            "deletion flag mapping with PARTITION BY: 'false' -> INSERT, 'true' -> DELETE")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema("id INT", "deleted STRING", "name STRING")
                                    .producedValues(
                                            Row.of(1, "false", "Alice"),
                                            Row.of(2, "false", "Bob"),
                                            Row.of(2, "true", "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream PARTITION BY id, "
                                    + "op => DESCRIPTOR(deleted), "
                                    + "op_mapping => MAP['false', 'INSERT', 'true', 'DELETE'])")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Table API test
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram TABLE_API_DEFAULT =
            TableTestProgram.of(
                            "from-changelog-table-api-default",
                            "Table.fromChangelog() convenience method")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedValues(
                                            Row.of(1, "INSERT", "Alice"),
                                            Row.of(2, "INSERT", "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"))
                                    .build())
                    .runTableApi(env -> env.from("cdc_stream").fromChangelog(), "sink")
                    .build();

    public static final TableTestProgram TABLE_API_RETRACT_PARTITION_BY =
            TableTestProgram.of(
                            "from-changelog-table-api-retract-partition-by",
                            "PartitionedTable.fromChangelog() convenience method")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedValues(
                                            Row.of(1, "INSERT", "Alice"),
                                            Row.of(2, "INSERT", "Bob"),
                                            Row.of(1, "UPDATE_BEFORE", "Alice"),
                                            Row.of(1, "UPDATE_AFTER", "Alice2"),
                                            Row.of(2, "DELETE", "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "Alice2"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .runTableApi(
                            env -> env.from("cdc_stream").partitionBy($("id")).fromChangelog(),
                            "sink")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Round-trip test: FROM_CHANGELOG(TO_CHANGELOG(table))
    // --------------------------------------------------------------------------------------------

    /** Verifies that FROM_CHANGELOG(TO_CHANGELOG(table)) recovers the original dynamic table. */
    public static final TableTestProgram ROUND_TRIP =
            TableTestProgram.of(
                            "from-changelog-round-trip",
                            "FROM_CHANGELOG(TO_CHANGELOG(table)) recovers original table")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema("id INT", "name STRING")
                                    .addMode(ChangelogMode.all())
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "Alice2"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "Alice2"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .setupSql(
                            "CREATE VIEW changelog_view AS "
                                    + "SELECT * FROM TO_CHANGELOG(input => TABLE orders)")
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE changelog_view)")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Restore tests
    // --------------------------------------------------------------------------------------------

    /**
     * Append source with retract op codes through FROM_CHANGELOG, split across a compiled-plan +
     * savepoint restore.
     */
    public static final TableTestProgram RETRACT_RESTORE =
            TableTestProgram.of(
                            "from-changelog-retract-restore",
                            "FROM_CHANGELOG over an append CDC source restores via compiled plan "
                                    + "+ savepoint")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.of(1, "INSERT", "Alice"),
                                            Row.of(2, "INSERT", "Bob"))
                                    .producedAfterRestore(
                                            Row.of(1, "UPDATE_BEFORE", "Alice"),
                                            Row.of(1, "UPDATE_AFTER", "Alice2"),
                                            Row.of(2, "DELETE", "Bob"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, 1, "Alice"),
                                            Row.ofKind(RowKind.INSERT, 2, "Bob"))
                                    .consumedAfterRestore(
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "Alice"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, "Alice2"),
                                            Row.ofKind(RowKind.DELETE, 2, "Bob"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream)")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Error validation tests
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram INVALID_OP_CODE =
            TableTestProgram.of(
                            "from-changelog-invalid-op-code",
                            "fails when input contains an op code not in the mapping")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedValues(Row.of(1, "UNKNOWN", "Alice"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream)",
                            TableRuntimeException.class,
                            "Received invalid op code 'UNKNOWN'")
                    .build();

    public static final TableTestProgram NULL_OP_CODE =
            TableTestProgram.of(
                            "from-changelog-null-op-code",
                            "fails when input contains a NULL op code")
                    .setupTableSource(
                            SourceTestStep.newBuilder("cdc_stream")
                                    .addSchema(SIMPLE_CDC_SCHEMA)
                                    .producedValues(Row.of(1, null, "Alice"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("id INT", "name STRING")
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink SELECT * FROM FROM_CHANGELOG("
                                    + "input => TABLE cdc_stream)",
                            TableRuntimeException.class,
                            "Received NULL op code")
                    .build();
}
