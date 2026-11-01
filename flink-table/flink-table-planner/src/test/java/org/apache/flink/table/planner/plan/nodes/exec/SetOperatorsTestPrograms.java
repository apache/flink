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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram}s for set operations with a projection on top. */
public class SetOperatorsTestPrograms {

    // Rows of t1 and t2 differ in x but share their y values, so they only match if x is
    // compared: on (x, y) EXCEPT ALL keeps (1, 10) and (2, 20), INTERSECT ALL keeps (1, 10).
    private static final SourceTestStep T1 =
            SourceTestStep.newBuilder("t1")
                    .addSchema("x INT", "y INT")
                    .producedValues(Row.of(1, 10), Row.of(1, 10), Row.of(2, 20))
                    .build();

    private static final SourceTestStep T2 =
            SourceTestStep.newBuilder("t2")
                    .addSchema("x INT", "y INT")
                    .producedValues(Row.of(1, 10), Row.of(3, 20))
                    .build();

    public static final TableTestProgram EXCEPT_ALL_WITH_OUTER_PROJECTION =
            TableTestProgram.of(
                            "except-all-with-outer-projection",
                            "projects a field away after EXCEPT ALL")
                    .setupTableSource(T1)
                    .setupTableSource(T2)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("y INT")
                                    .consumedValues(Row.of(10), Row.of(20))
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT y FROM "
                                    + "(SELECT x, y FROM t1 EXCEPT ALL SELECT x, y FROM t2)")
                    .build();

    public static final TableTestProgram INTERSECT_ALL_WITH_OUTER_PROJECTION =
            TableTestProgram.of(
                            "intersect-all-with-outer-projection",
                            "projects a field away after INTERSECT ALL")
                    .setupTableSource(T1)
                    .setupTableSource(T2)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("y INT")
                                    .consumedValues(Row.of(10))
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT y FROM "
                                    + "(SELECT x, y FROM t1 INTERSECT ALL SELECT x, y FROM t2)")
                    .build();

    // The planner can replace c by the constant of the left input, which is only valid above the
    // EXCEPT ALL: pushed into the right input it would make (1, 2) match (1, 1).
    public static final TableTestProgram EXCEPT_ALL_WITH_CONSTANT_COLUMN =
            TableTestProgram.of(
                            "except-all-with-constant-column",
                            "reorders a constant column after EXCEPT ALL")
                    .setupTableSource(T1)
                    .setupTableSource(T2)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("c INT", "x INT")
                                    .consumedValues(Row.of(1, 1), Row.of(1, 1), Row.of(1, 2))
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT c, x FROM "
                                    + "(SELECT x, 1 AS c FROM t1 EXCEPT ALL SELECT x, 2 AS c FROM t2)")
                    .build();

    public static final TableTestProgram EXCEPT_ALL_WITH_OUTER_PERMUTATION =
            TableTestProgram.of(
                            "except-all-with-outer-permutation",
                            "reorders all fields after EXCEPT ALL")
                    .setupTableSource(T1)
                    .setupTableSource(T2)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("y INT", "x INT")
                                    .consumedValues(Row.of(10, 1), Row.of(20, 2))
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT y, x FROM "
                                    + "(SELECT x, y FROM t1 EXCEPT ALL SELECT x, y FROM t2)")
                    .build();

    public static final TableTestProgram UNION_ALL_WITH_OUTER_PROJECTION =
            TableTestProgram.of(
                            "union-all-with-outer-projection",
                            "projects a field away after UNION ALL")
                    .setupTableSource(T1)
                    .setupTableSource(T2)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("y INT")
                                    .consumedValues(
                                            Row.of(10),
                                            Row.of(10),
                                            Row.of(20),
                                            Row.of(10),
                                            Row.of(20))
                                    .testMaterializedData()
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT y FROM "
                                    + "(SELECT x, y FROM t1 UNION ALL SELECT x, y FROM t2)")
                    .build();
}
