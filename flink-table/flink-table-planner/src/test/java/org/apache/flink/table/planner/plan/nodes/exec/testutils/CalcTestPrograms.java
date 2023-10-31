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

package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc1;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc2;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.UdfWithOpen;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/** {@link TableTestProgram} definitions for testing {@link StreamExecCalc}. */
public class CalcTestPrograms {

    static final TableTestProgram SIMPLE_CALC =
            TableTestProgram.of("calc-simple", "validates basic calc node")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .producedBeforeRestore(Row.of(420L, 42.0))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .consumedBeforeRestore(Row.of(421L, 42.0))
                                    .consumedAfterRestore(Row.of(422L, 42.1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a + 1, b FROM t")
                    .build();

    static final TableTestProgram CALC_PROJECT_PUSHDOWN =
            TableTestProgram.of(
                            "calc-project-pushdown", "validates calc node with project pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(421L, 42.1))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "a1 VARCHAR")
                                    .consumedBeforeRestore(Row.of(421L, "421"))
                                    .consumedAfterRestore(Row.of(421L, "421"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a, CAST(a AS VARCHAR) FROM source_t WHERE a > CAST(1 AS BIGINT)")
                    .build();

    static final TableTestProgram CALC_FILTER =
            TableTestProgram.of("calc-filter", "validates calc node with filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b INT", "c DOUBLE", "d VARCHAR")
                                    .producedBeforeRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .producedAfterRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b INT", "c DOUBLE", "d VARCHAR")
                                    .consumedBeforeRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .consumedAfterRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t WHERE b > 0")
                    .build();

    static final TableTestProgram CALC_FILTER_PUSHDOWN =
            TableTestProgram.of("calc-filter-pushdown", "validates calc node with filter pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(421L, 42.1))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .consumedBeforeRestore(Row.of(421L, 42.1))
                                    .consumedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a, b FROM source_t WHERE a > CAST(420 AS BIGINT)")
                    .build();

    static final TableTestProgram CALC_SARG =
            TableTestProgram.of("calc-sarg", "validates calc node with Sarg")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(1))
                                    .producedAfterRestore(Row.of(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore(Row.of(1))
                                    .consumedAfterRestore(Row.of(1))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a FROM source_t WHERE a = 1 or a = 2 or a is null")
                    .build();

    static final TableTestProgram CALC_UDF_SIMPLE =
            TableTestProgram.of("calc-udf-simple", "validates calc node with simple UDF")
                    .setupTemporaryCatalogFunction("udf1", JavaFunc0.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .producedBeforeRestore(Row.of(5))
                                    .producedAfterRestore(Row.of(5))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "a1 BIGINT")
                                    .consumedBeforeRestore(Row.of(5, 6L))
                                    .consumedAfterRestore(Row.of(5, 6L))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, udf1(a) FROM source_t")
                    .build();

    static final TableTestProgram CALC_UDF_COMPLEX =
            TableTestProgram.of("calc-udf-complex", "validates calc node with complex UDFs")
                    .setupTemporaryCatalogFunction("udf1", JavaFunc0.class)
                    .setupTemporaryCatalogFunction("udf2", JavaFunc1.class)
                    .setupTemporarySystemFunction("udf3", JavaFunc2.class)
                    .setupTemporarySystemFunction("udf4", UdfWithOpen.class)
                    .setupCatalogFunction("udf5", JavaFunc5.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a BIGINT, b INT NOT NULL, c VARCHAR, d TIMESTAMP(3)")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    5L,
                                                    11,
                                                    "hello world",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 1, 123)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    5L,
                                                    11,
                                                    "hello world",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 1, 123)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "a BIGINT",
                                            "a1 VARCHAR",
                                            "b INT NOT NULL",
                                            "b1 VARCHAR",
                                            "c1 VARCHAR",
                                            "c2 VARCHAR",
                                            "d1 TIMESTAMP(3)")
                                    .consumedBeforeRestore(
                                            Row.of(
                                                    5L,
                                                    "5",
                                                    11,
                                                    "11 and 11 and 1702688461000",
                                                    "hello world11",
                                                    "$hello",
                                                    LocalDateTime.of(2023, 12, 16, 01, 01, 00, 0)))
                                    .consumedAfterRestore(
                                            Row.of(
                                                    5L,
                                                    "5",
                                                    11,
                                                    "11 and 11 and 1702688461000",
                                                    "hello world11",
                                                    "$hello",
                                                    LocalDateTime.of(2023, 12, 16, 01, 01, 00, 0)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "a, "
                                    + "cast(a as VARCHAR) as a1, "
                                    + "b, "
                                    + "udf2(b, b, d) as b1, "
                                    + "udf3(c, b) as c1, "
                                    + "udf4(substring(c, 1, 5)) as c2, "
                                    + "udf5(d, 1000) as d1 "
                                    + "from source_t where "
                                    + "(udf1(a) > 0 or (a * b) < 100) and b > 10")
                    .build();
}
