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

/** {@link TableTestProgram} definitions for testing {@link StreamExecTemporalSort}. */
public class TemporalSortTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of("2020-10-10 00:00:01", 1, 1d),
        Row.of("2020-10-10 00:00:02", 2, 2d),
        Row.of("2020-10-10 00:00:07", 5, 6d),
        Row.of("2020-10-10 00:00:07", 3, 3d),
        // out of order
        Row.of("2020-10-10 00:00:06", 6, 6d),
        Row.of("2020-10-10 00:00:08", 3, null),
        // late event
        Row.of("2020-10-10 00:00:04", 5, 5d),
        Row.of("2020-10-10 00:00:16", 4, 4d)
    };

    static final Row[] AFTER_DATA = {
        Row.of("2020-10-10 00:00:40", 10, 3d), Row.of("2020-10-10 00:00:42", 11, 4d)
    };
    static final TableTestProgram TEMPORAL_SORT_PROCTIME =
            TableTestProgram.of(
                            "temporal-sort-proctime", "validates temporal sort node with proctime")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "c STRING",
                                            "`proctime` as PROCTIME()")
                                    .producedBeforeRestore(
                                            Row.of(1, 1L, "Hi"),
                                            Row.of(2, 2L, "Hello"),
                                            Row.of(3, 2L, "Hello world"))
                                    .producedAfterRestore(
                                            Row.of(4, 1L, "Guten Morgen"),
                                            Row.of(5, 2L, "Guten Tag"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore("+I[1]", "+I[2]", "+I[3]")
                                    .consumedAfterRestore("+I[4]", "+I[5]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a from source_t ORDER BY proctime")
                    .build();

    static final TableTestProgram TEMPORAL_SORT_ROWTIME =
            TableTestProgram.of(
                            "temporal-sort-rowtime", "validates temporal sort node with rowtime")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "ts STRING",
                                            "`int` INT",
                                            "`double` DOUBLE",
                                            "`rowtime` AS TO_TIMESTAMP(`ts`)",
                                            "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND")
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore(
                                            "+I[1]", "+I[2]", "+I[6]", "+I[3]", "+I[5]", "+I[3]")
                                    .consumedAfterRestore("+I[4]", "+I[10]", "+I[11]")
                                    .build())
                    .runSql(
                            "insert into sink_t SELECT `int` FROM source_t order by rowtime, `double`")
                    .build();
}
