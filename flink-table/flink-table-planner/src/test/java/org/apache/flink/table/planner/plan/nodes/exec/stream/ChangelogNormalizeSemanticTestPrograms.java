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

/**
 * {@link TableTestProgram} definitions for semantic testing {@link StreamExecChangelogNormalize}.
 */
public class ChangelogNormalizeSemanticTestPrograms {

    private static final String[] SINK_SCHEMA = {
        "a STRING", "b INT", "c STRING", "PRIMARY KEY(a) NOT ENFORCED"
    };
    private static final String[] SOURCE_SCHEMA = {
        "a STRING", "b INT", "c STRING", "PRIMARY KEY(a) NOT ENFORCED"
    };

    static final TableTestProgram UPSERT_SOURCE_WITH_FILTER =
            TableTestProgram.of("upsert-with-filter", "validates upsert with filter")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "UA,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "cc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+I[three, 3, ccc]",
                                            "+U[one, 4, aaaa]",
                                            "-D[three, 3, ccc]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE b < 10")
                    .build();

    static final TableTestProgram UPSERT_SOURCE_WITH_KEY_FILTER =
            TableTestProgram.of("upsert-key-filter", "validates upsert with key filter")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "UA,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.INSERT, "two", 1, "d"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "two", 1, "d"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "cc"),
                                            Row.ofKind(RowKind.DELETE, "two", 1, "d"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+U[one, 4, aaaa]",
                                            "+I[two, 1, d]",
                                            "-D[two, 1, d]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE a <> 'three'")
                    .build();

    static final TableTestProgram UPSERT_SOURCE =
            TableTestProgram.of("upsert", "validates changelog normalize")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "UA,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "cc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+I[three, 3, ccc]",
                                            "+U[one, 4, aaaa]",
                                            "-D[three, 3, ccc]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t")
                    .build();

    static final TableTestProgram CHANGELOG_NORMALIZE_WITH_FILTER =
            TableTestProgram.of(
                            "changelog-normalize-filter",
                            "validates changelog normalize with filter")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "I,UB,UA,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "cc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+I[three, 3, ccc]",
                                            "+U[one, 4, aaaa]",
                                            "-D[three, 3, ccc]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE b < 10")
                    .build();

    static final TableTestProgram CHANGELOG_NORMALIZE_WITH_KEY_FILTER =
            TableTestProgram.of(
                            "changelog-normalize-key-filter",
                            "validates changelog normalize with key filter")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "I,UB,UA,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.INSERT, "two", 1, "d"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "two", 1, "d"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "cc"),
                                            Row.ofKind(RowKind.DELETE, "two", 1, "d"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+U[one, 4, aaaa]",
                                            "+I[two, 1, d]",
                                            "-D[two, 1, d]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE a <> 'three'")
                    .build();

    static final TableTestProgram CHANGELOG_NORMALIZE =
            TableTestProgram.of("changelog-normalize", "validates changelog normalize")
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("changelog-mode", "I,UB,UA,D")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "cc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+I[three, 3, ccc]",
                                            "+U[one, 4, aaaa]",
                                            "-D[three, 3, ccc]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t")
                    .build();
}
