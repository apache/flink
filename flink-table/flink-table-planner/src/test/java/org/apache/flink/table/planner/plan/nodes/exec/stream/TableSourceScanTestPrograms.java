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

/** {@link TableTestProgram} definitions for testing {@link StreamExecTableSourceScan}. */
public class TableSourceScanTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of(1, 1L, "hi", DateTimeUtils.toLocalDateTime(1586937601000L)),
        Row.of(2, 2L, "hello", DateTimeUtils.toLocalDateTime(1586937602000L)),
        Row.of(3, 2L, "hello world", DateTimeUtils.toLocalDateTime(1586937603000L))
    };

    static final Row[] AFTER_DATA = {
        Row.of(4, 4L, "foo", DateTimeUtils.toLocalDateTime(1586937614000L)),
        Row.of(5, 2L, "foo bar", DateTimeUtils.toLocalDateTime(1586937615000L)),
    };

    static final TableTestProgram PROJECT_PUSHDOWN =
            TableTestProgram.of(
                            "table-source-scan-project-pushdown",
                            "validates table source scan with project pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[1, hi]", "+I[2, hello]", "+I[3, hello world]")
                                    .consumedAfterRestore("+I[4, foo]", "+I[5, foo bar]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, c FROM source_t")
                    .build();

    static final TableTestProgram PROJECT_PUSHDOWN_DISABLED =
            TableTestProgram.of(
                            "table-source-scan-project-push-down-disabled",
                            "validates table source scan with project pushdown disabled")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "c VARCHAR",
                                            "ts TIMESTAMP(3) METADATA")
                                    .addOption("readable-metadata", "ts:TIMESTAMP(3)")
                                    .addOption("enable-projection-push-down", "false")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "c VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[1, hi]", "+I[2, hello]", "+I[3, hello world]")
                                    .consumedAfterRestore("+I[4, foo]", "+I[5, foo bar]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, c FROM source_t")
                    .build();

    static final TableTestProgram FILTER_PUSHDOWN =
            TableTestProgram.of(
                            "table-source-scan-filter-pushdown",
                            "validates table source scan with filter pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[2, 2, hello]", "+I[3, 2, hello world]")
                                    .consumedAfterRestore("+I[4, 4, foo]", "+I[5, 2, foo bar]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t WHERE a > 1")
                    .build();

    static final TableTestProgram LIMIT_PUSHDOWN =
            TableTestProgram.of(
                            "table-source-scan-limit-pushdown",
                            "validates table source scan with limit pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .consumedBeforeRestore("+I[1, 1, hi]", "+I[2, 2, hello]")
                                    .consumedAfterRestore(new String[] {})
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t LIMIT 2")
                    .build();

    static final TableTestProgram PARTITION_PUSHDOWN =
            TableTestProgram.of(
                            "table-source-scan-partition-pushdown",
                            "validates table source scan with partition pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .addPartitionKeys("b")
                                    .addOption("partition-list", "b:1,b:2")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[2, 2, hello]", "+I[3, 2, hello world]")
                                    .consumedAfterRestore("+I[5, 2, foo bar]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b, c FROM source_t WHERE b = 2")
                    .build();

    static final TableTestProgram READING_METADATA =
            TableTestProgram.of(
                            "table-source-scan-reading-metadata",
                            "validates table source scan by reading metadata")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "c VARCHAR",
                                            "d TIMESTAMP(3) METADATA")
                                    .addOption("readable-metadata", "d:TIMESTAMP(3)")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "c VARCHAR", "d TIMESTAMP(3)")
                                    .consumedBeforeRestore(
                                            "+I[1, hi, 2020-04-15T08:00:01]",
                                            "+I[2, hello, 2020-04-15T08:00:02]",
                                            "+I[3, hello world, 2020-04-15T08:00:03]")
                                    .consumedAfterRestore(
                                            "+I[4, foo, 2020-04-15T08:00:14]",
                                            "+I[5, foo bar, 2020-04-15T08:00:15]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, c, d FROM source_t")
                    .build();

    static final TableTestProgram MULTIPLE_PUSHDOWNS =
            TableTestProgram.of(
                            "table-source-scan-multiple-pushdowns",
                            "validates table source scan with multiple pushdowns")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "c VARCHAR",
                                            "ts TIMESTAMP(3) METADATA",
                                            "watermark for ts as ts - interval '1' second")
                                    .addOption("readable-metadata", "ts:TIMESTAMP(3)")
                                    .addOption("filterable-fields", "a")
                                    .addOption("enable-watermark-push-down", "true")
                                    .addOption("disable-lookup", "true")
                                    .addOption("partition-list", "b:1;b:2;b:3;b:4;b:5;b:6")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore("+I[3]")
                                    .consumedAfterRestore("+I[5]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a FROM source_t WHERE b = 2 AND a > 2")
                    .build();

    static final TableTestProgram SOURCE_WATERMARK =
            TableTestProgram.of(
                            "table-source-scan-source-watermark",
                            "validates table source scan using source watermark")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "c VARCHAR",
                                            "ts TIMESTAMP(3) METADATA",
                                            "watermark for ts as SOURCE_WATERMARK()")
                                    .addOption("readable-metadata", "ts:TIMESTAMP(3)")
                                    .addOption("enable-watermark-push-down", "true")
                                    .addOption("disable-lookup", "true")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "c VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[1, hi]", "+I[2, hello]", "+I[3, hello world]")
                                    .consumedAfterRestore("+I[4, foo]", "+I[5, foo bar]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, c FROM source_t")
                    .build();

    static final TableTestProgram REUSE_SOURCE =
            TableTestProgram.of(
                            "table-source-scan-reuse-source",
                            "validates table source scan by verifying if source is resused")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR", "d TIMESTAMP(3)")
                                    .addOption("enable-projection-push-down", "false")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_one_t")
                                    .addSchema("a INT", "c VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[1, hi]", "+I[2, hello]", "+I[3, hello world]")
                                    .consumedAfterRestore("+I[4, foo]", "+I[5, foo bar]")
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_two_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .consumedBeforeRestore("+I[1, 1]", "+I[2, 2]", "+I[3, 2]")
                                    .consumedAfterRestore("+I[4, 4]", "+I[5, 2]")
                                    .build())
                    .runStatementSet(
                            "INSERT INTO sink_one_t SELECT a, c FROM source_t",
                            "INSERT INTO sink_two_t SELECT a, b FROM source_t")
                    .build();
}
