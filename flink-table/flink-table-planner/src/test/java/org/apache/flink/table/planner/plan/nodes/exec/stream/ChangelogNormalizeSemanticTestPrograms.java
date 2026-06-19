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

    private static final ChangelogMode KAFKA_WITH_PARTIAL_DELETES_CHANGELOG =
            ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .addContainedKind(RowKind.DELETE)
                    .keyOnlyDeletes(true)
                    .build();

    static final TableTestProgram UPSERT_SOURCE_WITH_NON_KEY_FILTER =
            TableTestProgram.of(
                            "upsert-with-non-key-filter", "validates upsert with non key filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(ChangelogMode.upsert())
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "two", 0, "dd"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "two", 1, "dd"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", 5, "ccc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "-U[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "-D[one, 2, bb]",
                                            "+I[two, 0, dd]",
                                            "-U[two, 0, dd]",
                                            "+U[two, 1, dd]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE b < 3")
                    .build();

    static final TableTestProgram UPSERT_SOURCE_WITH_KEY_FILTER =
            TableTestProgram.of("upsert-with-key-filter", "validates upsert with key filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(ChangelogMode.upsert())
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
                                            Row.ofKind(RowKind.DELETE, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.DELETE, "two", 1, "d"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "-U[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "-U[one, 2, bb]",
                                            "+U[one, 4, aaaa]",
                                            "+I[two, 1, d]",
                                            "-D[two, 1, d]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE a <> 'three'")
                    .build();

    static final TableTestProgram UPSERT_SOURCE_WITH_NO_FILTER =
            TableTestProgram.of("upsert-with-no-filter", "validates upsert with no filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(ChangelogMode.upsert())
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.INSERT, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "cc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "-U[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "-U[one, 2, bb]",
                                            "+U[one, 4, aaaa]",
                                            "+I[three, 3, ccc]",
                                            "-D[three, 3, ccc]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t")
                    .build();

    static final TableTestProgram KAFKA_SOURCE_WITH_NON_KEY_FILTER =
            TableTestProgram.of(
                            "kafka-with-non-key-filter",
                            "validates kafka source (with partial deletes) with non key filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(KAFKA_WITH_PARTIAL_DELETES_CHANGELOG)
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "two", 0, "dd"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "two", 1, "dd"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", null, null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "-U[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "-D[one, 2, bb]",
                                            "+I[two, 0, dd]",
                                            "-U[two, 0, dd]",
                                            "+U[two, 1, dd]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE b < 3")
                    .build();

    static final TableTestProgram KAFKA_SOURCE_WITH_KEY_FILTER =
            TableTestProgram.of(
                            "kafka-with-key-filter",
                            "validates kafka source (with partial deletes) with key filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(KAFKA_WITH_PARTIAL_DELETES_CHANGELOG)
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
                                            Row.ofKind(RowKind.DELETE, "three", null, null),
                                            Row.ofKind(RowKind.DELETE, "two", null, null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "-U[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "-U[one, 2, bb]",
                                            "+U[one, 4, aaaa]",
                                            "+I[two, 1, d]",
                                            "-D[two, 1, d]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE a <> 'three'")
                    .build();

    static final TableTestProgram KAFKA_SOURCE_WITH_NO_FILTER =
            TableTestProgram.of(
                            "kafka-with-no-filter",
                            "validates kafka source (with partial deletes) with no filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(KAFKA_WITH_PARTIAL_DELETES_CHANGELOG)
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.INSERT, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", null, null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "-U[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "-U[one, 2, bb]",
                                            "+U[one, 4, aaaa]",
                                            "+I[three, 3, ccc]",
                                            "-D[three, 3, ccc]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t")
                    .build();

    static final TableTestProgram RETRACT_SOURCE_WITH_NON_KEY_FILTER =
            TableTestProgram.of(
                            "retract-source-with-non-key-filter",
                            "validates retract source with non filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "ccc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .addMode(ChangelogMode.all())
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+U[three, 3, ccc]",
                                            "+U[three, 3, ccc]",
                                            "+I[one, 4, aaaa]",
                                            "+U[one, 4, aaaa]",
                                            "-D[three, 3, ccc]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE b < 10")
                    .build();

    static final TableTestProgram RETRACT_SOURCE_WITH_KEY_FILTER =
            TableTestProgram.of(
                            "retract-source-with-key-filter",
                            "validates retract source with key filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(ChangelogMode.all())
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
                                    .addMode(ChangelogMode.all())
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+I[one, 4, aaaa]",
                                            "+U[one, 4, aaaa]",
                                            "+I[two, 1, d]",
                                            "+U[two, 1, d]",
                                            "-D[two, 1, d]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE a <> 'three'")
                    .build();

    static final TableTestProgram RETRACT_SOURCE_NO_FILTER =
            TableTestProgram.of(
                            "retract-source-with-no-filter",
                            "validates retract source with no filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "one", 1, "a"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 2, "bb"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "three", 3, "ccc"),
                                            Row.ofKind(RowKind.INSERT, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "one", 4, "aaaa"),
                                            Row.ofKind(RowKind.DELETE, "three", 3, "ccc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addMode(ChangelogMode.all())
                                    .addSchema(SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[one, 1, a]",
                                            "+U[one, 2, bb]",
                                            "+U[three, 3, ccc]",
                                            "+I[one, 4, aaaa]",
                                            "+U[one, 4, aaaa]",
                                            "+U[three, 3, ccc]",
                                            "-D[three, 3, ccc]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t")
                    .build();
}
