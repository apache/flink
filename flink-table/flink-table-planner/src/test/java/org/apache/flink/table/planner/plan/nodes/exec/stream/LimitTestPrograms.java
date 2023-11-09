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

/**
 * {@link TableTestProgram} definitions for testing {@link
 * org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLimit}.
 */
public class LimitTestPrograms {

    static final Row[] DATA1 =
            new Row[] {
                Row.of(2, "a", 6),
                Row.of(4, "b", 8),
                Row.of(6, "c", 10),
                Row.of(1, "a", 5),
                Row.of(3, "b", 7),
                Row.of(5, "c", 9)
            };

    static final Row[] DATA2 = new Row[] {Row.of(8, "d", 3), Row.of(7, "e", 2)};
    static final TableTestProgram LIMIT =
            TableTestProgram.of("limit", "validates limit node")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b VARCHAR", "c INT")
                                    .producedBeforeRestore(DATA1)
                                    .producedAfterRestore(DATA2)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b VARCHAR", "c BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[2, a, 6]", "+I[4, b, 8]", "+I[6, c, 10]")
                                    .consumedAfterRestore(new String[] {})
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * from source_t LIMIT 3")
                    .build();
}
