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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Duration;

/** {@link TableTestProgram} definitions for testing {@link StreamExecChangelogNormalize}. */
public class ChangelogNormalizeTestPrograms {

    static final String[] SOURCE_SCHEMA = {
        "a VARCHAR", "b INT NOT NULL", "c VARCHAR", "PRIMARY KEY(a) NOT ENFORCED"
    };

    static final String[] SINK_SCHEMA = {"a VARCHAR", "b INT", "c VARCHAR"};

    static final Row[] BEFORE_DATA = {
        Row.ofKind(RowKind.INSERT, "one", 1, "a"),
        Row.ofKind(RowKind.INSERT, "two", 2, "b"),
        Row.ofKind(RowKind.UPDATE_BEFORE, "one", 1, "a"),
        Row.ofKind(RowKind.UPDATE_AFTER, "one", 1, "aa"),
        Row.ofKind(RowKind.INSERT, "three", 3, "c"),
        Row.ofKind(RowKind.DELETE, "two", 2, "b"),
        Row.ofKind(RowKind.UPDATE_BEFORE, "three", 3, "c"),
        Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "cc"),
    };

    static final Row[] AFTER_DATA = {
        Row.ofKind(RowKind.INSERT, "four", 4, "d"),
        Row.ofKind(RowKind.INSERT, "five", 5, "e"),
        Row.ofKind(RowKind.UPDATE_BEFORE, "four", 4, "d"),
        Row.ofKind(RowKind.UPDATE_AFTER, "four", 4, "dd"),
        Row.ofKind(RowKind.INSERT, "six", 6, "f"),
        Row.ofKind(RowKind.DELETE, "six", 6, "f")
    };

    static final String[] BEFORE_OUTPUT = {
        "+I[one, 1, a]",
        "+I[two, 2, b]",
        "-U[one, 1, a]",
        "+U[one, 1, aa]",
        "+I[three, 3, c]",
        "-D[two, 2, b]",
        "-U[three, 3, c]",
        "+U[three, 3, cc]"
    };

    static final String[] AFTER_OUTPUT = {
        "+I[four, 4, d]",
        "+I[five, 5, e]",
        "-U[four, 4, d]",
        "+U[four, 4, dd]",
        "+I[six, 6, f]",
        "-D[six, 6, f]"
    };

    static final TableTestProgram CHANGELOG_SOURCE =
            TableTestProgram.of(
                            "changelog-normalize-source", "validates changelog normalize source")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "I,UA,UB,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(BEFORE_OUTPUT)
                                    .consumedAfterRestore(AFTER_OUTPUT)
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t")
                    .build();

    static final TableTestProgram CHANGELOG_SOURCE_MINI_BATCH =
            TableTestProgram.of(
                            "changelog-normalize-source-mini-batch",
                            "validates changelog normalize source with mini batch")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 2L)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "I,UA,UB,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(BEFORE_OUTPUT)
                                    .consumedAfterRestore(AFTER_OUTPUT)
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t")
                    .build();

    static final TableTestProgram UPSERT_SOURCE =
            TableTestProgram.of(
                            "changelog-normalize-upsert", "validates changelog normalize upsert")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "two", 2, "b"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 1, "aa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "c"),
                                            Row.ofKind(RowKind.DELETE, "two", 2, "b"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "cc"))
                                    .producedAfterRestore(
                                            Row.ofKind(RowKind.UPDATE_AFTER, "four", 4, "d"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "five", 5, "e"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "six", 6, "f"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "five", 5, "ee"),
                                            Row.ofKind(RowKind.DELETE, "six", 6, "f"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "four", 4, "dd"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(BEFORE_OUTPUT)
                                    .consumedAfterRestore(
                                            "+I[four, 4, d]",
                                            "+I[five, 5, e]",
                                            "+I[six, 6, f]",
                                            "-U[five, 5, e]",
                                            "+U[five, 5, ee]",
                                            "-D[six, 6, f]",
                                            "-U[four, 4, d]",
                                            "+U[four, 4, dd]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t")
                    .build();
}
