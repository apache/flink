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

/** {@link TableTestProgram} definitions for testing the built-in FROM_CHANGELOG PTF. */
public class FromChangelogTestPrograms {

    private static final String[] SIMPLE_CDC_SCHEMA = {"id INT", "op STRING", "name STRING"};

    // --------------------------------------------------------------------------------------------
    // SQL tests
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram DEFAULT_OP_MAPPING =
            TableTestProgram.of(
                            "from-changelog-default-op-mapping",
                            "default mapping with standard op names")
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

    public static final TableTestProgram UNMAPPED_CODES_DROPPED =
            TableTestProgram.of(
                            "from-changelog-unmapped-codes-dropped",
                            "unmapped op codes are silently dropped")
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
                                    + "input => TABLE cdc_stream)")
                    .build();

    /** Custom op column name via DESCRIPTOR. */
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
}
