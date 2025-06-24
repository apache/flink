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
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecWindowRank}. */
public class WatermarkAssignerTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of(
                2,
                2L,
                "Hello",
                "2020-04-15 08:00:00",
                DateTimeUtils.toLocalDateTime(1586937600000L)),
        Row.of(1, 1L, "Hi", "2020-04-15 08:00:01", DateTimeUtils.toLocalDateTime(1586937601000L)),
        Row.of(
                3,
                2L,
                "Hello world",
                "2020-04-15 08:00:02",
                DateTimeUtils.toLocalDateTime(1586937602000L)),
        Row.of(
                4,
                3L,
                "Hello world, how are you?",
                "2020-04-15 08:00:03",
                DateTimeUtils.toLocalDateTime(1586937603000L)),
        Row.of(
                5,
                3L,
                "I am fine.",
                "2020-04-15 08:00:04",
                DateTimeUtils.toLocalDateTime(1586937604000L)),
    };

    static final Row[] AFTER_DATA = {
        Row.of(7, 4L, "Ack", "2020-04-15 08:00:21", DateTimeUtils.toLocalDateTime(1586937621000L)),
        Row.of(6, 5L, "Syn", "2020-04-15 08:00:23", DateTimeUtils.toLocalDateTime(1586937623000L)),
        Row.of(
                8,
                3L,
                "Syn-Ack",
                "2020-04-15 08:00:25",
                DateTimeUtils.toLocalDateTime(1586937625000L)),
        Row.of(
                10,
                3L,
                "Close",
                "2020-04-15 08:00:28",
                DateTimeUtils.toLocalDateTime(1586937628000L))
    };

    static final String[] SOURCE_SCHEMA = {
        "a INT",
        "b BIGINT",
        "c VARCHAR",
        "ts_string STRING",
        "ts TIMESTAMP(3)", // row_time
        "WATERMARK for ts AS ts - INTERVAL '1' SECOND"
    };

    static final String[] SINK_SCHEMA = {"a INT", "b BIGINT", "ts TIMESTAMP(3)"};

    static final TableTestProgram WATERMARK_ASSIGNER_BASIC_FILTER =
            TableTestProgram.of(
                            "watermark-assigner-basic-filter",
                            "validates watermark assigner with basic filtering")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[4, 3, 2020-04-15T08:00:03]",
                                            "+I[5, 3, 2020-04-15T08:00:04]")
                                    .consumedAfterRestore(
                                            "+I[8, 3, 2020-04-15T08:00:25]",
                                            "+I[10, 3, 2020-04-15T08:00:28]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, ts FROM source_t WHERE b = 3")
                    .build();

    static final TableTestProgram WATERMARK_ASSIGNER_PUSHDOWN_METADATA =
            TableTestProgram.of(
                            "watermark-assigner-pushdown-metadata",
                            "validates watermark assigner with pushdown metadata")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addOption("enable-watermark-push-down", "true")
                                    .addOption("readable-metadata", "ts:timestamp(3)")
                                    .addOption("disable-lookup", "true")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[4, 3, 2020-04-15T08:00:03]",
                                            "+I[5, 3, 2020-04-15T08:00:04]")
                                    .consumedAfterRestore(
                                            "+I[8, 3, 2020-04-15T08:00:25]",
                                            "+I[10, 3, 2020-04-15T08:00:28]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, ts FROM source_t WHERE b = 3")
                    .build();
}
