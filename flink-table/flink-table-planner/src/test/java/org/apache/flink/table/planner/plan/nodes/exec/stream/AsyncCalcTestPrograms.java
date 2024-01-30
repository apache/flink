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

import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

public class AsyncCalcTestPrograms {

    static final TableTestProgram ASYNC_CALC_UDF_SIMPLE =
            TableTestProgram.of("async-calc-simple", "validates async calc node with simple UDF")
                    .setupTemporaryCatalogFunction(
                            "udf1", JavaUserDefinedScalarFunctions.AsyncJavaFunc0.class)
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

    static final TableTestProgram ASYNC_CALC_UDF_COMPLEX =
            TableTestProgram.of("async-calc-complex", "validates calc node with complex UDFs")
                    .setupTemporaryCatalogFunction(
                            "udf1", JavaUserDefinedScalarFunctions.AsyncJavaFunc0.class)
                    .setupTemporaryCatalogFunction(
                            "udf2", JavaUserDefinedScalarFunctions.AsyncJavaFunc1.class)
                    .setupTemporarySystemFunction(
                            "udf3", JavaUserDefinedScalarFunctions.AsyncJavaFunc2.class)
                    .setupTemporarySystemFunction(
                            "udf4", JavaUserDefinedScalarFunctions.AsyncUdfWithOpen.class)
                    .setupCatalogFunction(
                            "udf5", JavaUserDefinedScalarFunctions.AsyncJavaFunc5.class)
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

    static final TableTestProgram ASYNC_CALC_UDF_NESTED =
            TableTestProgram.of("async-calc-nested", "validates async calc node with simple UDF")
                    .setupTemporaryCatalogFunction(
                            "udf1", JavaUserDefinedScalarFunctions.AsyncJavaFunc0.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .producedBeforeRestore(Row.of(5))
                                    .producedAfterRestore(Row.of(5))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "a1 BIGINT")
                                    .consumedBeforeRestore(Row.of(5, 8L))
                                    .consumedAfterRestore(Row.of(5, 8L))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, udf1(udf1(udf1(a))) FROM source_t")
                    .build();

    static final TableTestProgram ASYNC_CALC_UDF_CONDITION =
            TableTestProgram.of("async-calc-condition", "validates async calc node with simple UDF")
                    .setupTemporaryCatalogFunction(
                            "udf1", JavaUserDefinedScalarFunctions.AsyncJavaFunc0.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .producedBeforeRestore(Row.of(5), Row.of(6), Row.of(4))
                                    .producedAfterRestore(Row.of(7), Row.of(3))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore(Row.of(6))
                                    .consumedAfterRestore(Row.of(7))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a FROM source_t WHERE udf1(a) > 6")
                    .build();
}
