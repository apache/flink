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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_ATTEMPTS;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_RETRY_DELAY;
import static org.assertj.core.api.Assertions.fail;

/** {@link TableTestProgram} definitions for testing {@link StreamExecAsyncCalc}. */
public class AsyncCalcTestPrograms {

    static final TableTestProgram ASYNC_CALC_UDF_SIMPLE =
            TableTestProgram.of("async-calc-simple", "validates async calc node with simple UDF")
                    .setupTemporaryCatalogFunction("udf1", AsyncJavaFunc0.class)
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
                    .setupTemporaryCatalogFunction("udf1", AsyncJavaFunc0.class)
                    .setupTemporaryCatalogFunction("udf2", AsyncJavaFunc1.class)
                    .setupTemporarySystemFunction("udf3", AsyncJavaFunc2.class)
                    .setupTemporarySystemFunction("udf4", AsyncUdfWithOpen.class)
                    .setupCatalogFunction("udf5", AsyncJavaFunc5.class)
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
            TableTestProgram.of(
                            "async-calc-nested",
                            "validates async calc node when chained through nested calls")
                    .setupTemporaryCatalogFunction("udf1", AsyncJavaFunc0.class)
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
            TableTestProgram.of(
                            "async-calc-condition",
                            "validates async calc node with the udf written in the condition of the SQL query")
                    .setupTemporaryCatalogFunction("udf1", AsyncJavaFunc0.class)
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

    static final TableTestProgram ASYNC_CALC_UDF_FAILURE_EXCEPTION =
            TableTestProgram.of(
                            "async-calc-failure-exception",
                            "validates async calc node that fails some number of times and then recovers after restore")
                    .setupConfig(TABLE_EXEC_ASYNC_SCALAR_RETRY_DELAY, Duration.ofMillis(3000))
                    .setupConfig(TABLE_EXEC_ASYNC_SCALAR_MAX_ATTEMPTS, 3)
                    .setupConfig(TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY, 5)
                    .setupTemporaryCatalogFunction("udf1", TwosFailFunction.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .producedBeforeRestore(Row.of(1), Row.of(2))
                                    .producedAfterRestore(Row.of(3))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore(Row.of(1))
                                    .consumedAfterRestore(Row.of(2), Row.of(3))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT udf1(a) FROM source_t")
                    .build();

    /** Increment input. */
    public static class AsyncJavaFunc0 extends AsyncScalarFunction {
        public void eval(CompletableFuture<Long> future, Long l) {
            future.complete(l + 1);
        }
    }

    /** Concatenate inputs as strings. */
    public static class AsyncJavaFunc1 extends AsyncScalarFunction {
        public void eval(
                CompletableFuture<String> future,
                Integer a,
                int b,
                @DataTypeHint("TIMESTAMP(3)") TimestampData c) {
            Long ts = (c == null) ? null : c.getMillisecond();
            future.complete(a + " and " + b + " and " + ts);
        }
    }

    /** Append product to string. */
    public static class AsyncJavaFunc2 extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> future, String s, Integer... a) {
            int m = 1;
            for (int n : a) {
                m *= n;
            }
            future.complete(s + m);
        }
    }

    /**
     * A UDF minus Timestamp with the specified offset. This UDF also ensures open and close are
     * called.
     */
    public static class AsyncJavaFunc5 extends AsyncScalarFunction {
        // these fields must be reset to false at the beginning of tests,
        // otherwise the static fields will be changed by several tests concurrently
        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        @Override
        public void open(FunctionContext context) {
            openCalled = true;
        }

        public void eval(
                @DataTypeHint("TIMESTAMP(3)") CompletableFuture<Timestamp> future,
                @DataTypeHint("TIMESTAMP(3)") TimestampData timestampData,
                Integer offset) {
            if (!openCalled) {
                fail("Open was not called before run.");
            }
            if (timestampData == null || offset == null) {
                future.complete(null);
            } else {
                long ts = timestampData.getMillisecond() - offset;
                int tzOffset = TimeZone.getDefault().getOffset(ts);
                future.complete(new Timestamp(ts - tzOffset));
            }
        }

        @Override
        public void close() {
            closeCalled = true;
        }
    }

    /** Testing open method is called. */
    public static class AsyncUdfWithOpen extends AsyncScalarFunction {

        // transient make this class serializable by class name
        private transient boolean isOpened = false;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            this.isOpened = true;
        }

        public void eval(CompletableFuture<String> future, String c) {
            if (!isOpened) {
                throw new IllegalStateException("Open method is not called!");
            }
            future.complete("$" + c);
        }
    }

    /** Testing that failing requests can recover after restore. */
    public static class TwosFailFunction extends AsyncScalarFunction {
        private static final int TOTAL_FAILURES = 1;

        private final AtomicInteger calls = new AtomicInteger(0);

        public void eval(CompletableFuture<Integer> future, Integer c) {
            if (c != 2) {
                future.complete(c);
                return;
            }
            if (calls.incrementAndGet() > TOTAL_FAILURES) {
                future.complete(c);
                return;
            }
            throw new RuntimeException("Failure " + calls.get());
        }
    }
}
