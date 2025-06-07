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

import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.AsyncStringSplit;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_TABLE_MAX_CONCURRENT_OPERATIONS;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_TABLE_MAX_RETRIES;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_TABLE_RETRY_DELAY;

/** {@link TableTestProgram} definitions for testing {@link StreamExecAsyncCorrelate}. */
public class AsyncCorrelateTestPrograms {
    static final Row[] BEFORE_DATA = {Row.of(1L, 1, "hi#there"), Row.of(2L, 2, "hello#world")};

    static final Row[] AFTER_DATA = {
        Row.of(4L, 4, "foo#bar"), Row.of(3L, 3, "bar#fiz"),
    };

    static final String[] SOURCE_SCHEMA = {"a BIGINT", "b INT NOT NULL", "c VARCHAR"};

    public static final TableTestProgram CORRELATE_CATALOG_FUNC =
            TableTestProgram.of(
                            "async-correlate-catalog-func",
                            "validate correlate with temporary catalog function")
                    .setupTemporaryCatalogFunction("func1", TableFunc1.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hi#there, $hi]",
                                            "+I[hi#there, $there]",
                                            "+I[hello#world, $hello]",
                                            "+I[hello#world, $world]")
                                    .consumedAfterRestore(
                                            "+I[foo#bar, $foo]",
                                            "+I[foo#bar, $bar]",
                                            "+I[bar#fiz, $bar]",
                                            "+I[bar#fiz, $fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT c, s FROM source_t, LATERAL TABLE(func1(c, '$')) AS T(s)")
                    .build();

    public static final TableTestProgram CORRELATE_SYSTEM_FUNC =
            TableTestProgram.of(
                            "async-correlate-system-func",
                            "validate correlate with temporary system function")
                    .setupTemporarySystemFunction("STRING_SPLIT", AsyncStringSplit.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hi#there, hi]",
                                            "+I[hi#there, there]",
                                            "+I[hello#world, hello]",
                                            "+I[hello#world, world]")
                                    .consumedAfterRestore(
                                            "+I[foo#bar, foo]",
                                            "+I[foo#bar, bar]",
                                            "+I[bar#fiz, bar]",
                                            "+I[bar#fiz, fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT c, s FROM source_t, LATERAL TABLE(STRING_SPLIT(c, '#')) AS T(s)")
                    .build();

    public static final TableTestProgram CORRELATE_JOIN_FILTER =
            TableTestProgram.of(
                            "async-correlate-join-filter",
                            "validate correlate with join and filter")
                    .setupTemporaryCatalogFunction("func1", TableFunc1.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hello#world, hello]", "+I[hello#world, world]")
                                    .consumedAfterRestore("+I[bar#fiz, bar]", "+I[bar#fiz, fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM (SELECT c, s FROM source_t, LATERAL TABLE(func1(c)) AS T(s)) AS T2 WHERE c LIKE '%hello%' OR c LIKE '%fiz%'")
                    .build();

    public static final TableTestProgram CORRELATE_LEFT_JOIN =
            TableTestProgram.of("async-correlate-left-join", "validate correlate with left join")
                    .setupTemporaryCatalogFunction("func1", TableFunc1.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hi#there, hi]",
                                            "+I[hi#there, there]",
                                            "+I[hello#world, hello]",
                                            "+I[hello#world, world]")
                                    .consumedAfterRestore(
                                            "+I[foo#bar, foo]",
                                            "+I[foo#bar, bar]",
                                            "+I[bar#fiz, bar]",
                                            "+I[bar#fiz, fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT c, s FROM source_t LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON TRUE")
                    .build();

    public static final TableTestProgram CORRELATE_UDF_EXCEPTION =
            TableTestProgram.of(
                            "async-correlate-exception",
                            "validates async calc node that fails some number of times and then recovers after restore")
                    .setupConfig(TABLE_EXEC_ASYNC_TABLE_RETRY_DELAY, Duration.ofMillis(3000))
                    .setupConfig(TABLE_EXEC_ASYNC_TABLE_MAX_RETRIES, 3)
                    .setupConfig(TABLE_EXEC_ASYNC_TABLE_MAX_CONCURRENT_OPERATIONS, 5)
                    .setupTemporaryCatalogFunction("func1", FailureThenSucceedSplit.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    // Errors on "hello#world"
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hi#there, $hi]", "+I[hi#there, $there]")
                                    .consumedAfterRestore(
                                            "+I[hello#world, $hello]",
                                            "+I[hello#world, $world]",
                                            "+I[foo#bar, $foo]",
                                            "+I[foo#bar, $bar]",
                                            "+I[bar#fiz, $bar]",
                                            "+I[bar#fiz, $fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT c, s FROM source_t, LATERAL TABLE(func1(c, '$')) AS T(s)")
                    .build();

    /** Splitter functions. */
    public static class TableFunc1 extends AsyncTableFunction<String> {

        private static final long serialVersionUID = -7888476374311468525L;

        public void eval(CompletableFuture<Collection<String>> future, String str) {
            if (str.contains("#")) {
                List<String> result = Arrays.stream(str.split("#")).collect(Collectors.toList());
                future.complete(result);
            } else {
                future.complete(Collections.emptyList());
            }
        }

        public void eval(CompletableFuture<Collection<String>> future, String str, String prefix) {
            if (str.contains("#")) {
                List<String> result =
                        Arrays.stream(str.split("#"))
                                .map(s -> prefix + s)
                                .collect(Collectors.toList());
                future.complete(result);
            } else {
                future.complete(Collections.emptyList());
            }
        }
    }

    /** Fails then succeeds to test succeeding after restore. */
    public static class FailureThenSucceedSplit extends AsyncTableFunction<String> {
        private static final int TOTAL_FAILURES = 1;

        private final AtomicInteger calls = new AtomicInteger(0);

        public void eval(CompletableFuture<Collection<String>> future, String str, String prefix) {
            if (!str.equals("hello#world") || calls.incrementAndGet() > TOTAL_FAILURES) {
                if (str.contains("#")) {
                    List<String> result =
                            Arrays.stream(str.split("#"))
                                    .map(s -> prefix + s)
                                    .collect(Collectors.toList());
                    future.complete(result);
                } else {
                    future.complete(Collections.emptyList());
                }
            }
            throw new RuntimeException("Failure " + calls.get());
        }
    }
}
