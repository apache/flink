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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/**
 * {@link TableTestProgram} definitions for testing {@link
 * org.apache.flink.table.planner.plan.nodes.exec.batch.runtimefilter.BatchExecRuntimeFilter}.
 */
class RuntimeFilterTestPrograms {

    static final TableTestProgram RUNTIME_FILTER =
            TableTestProgram.of("runtime-filter", "validates runtime filter nodes")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_ENABLED, true)
                    // Without table statistics, each source is estimated at 1e8 rows. This puts the
                    // filtered build side at ~1.2 GB and the probe side at ~2.4 GB, so the
                    // thresholds have to bracket these sizes for a runtime filter to be injected.
                    .setupConfig(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_RUNTIME_FILTER_MAX_BUILD_DATA_SIZE,
                            MemorySize.parse("1500m"))
                    .setupConfig(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_RUNTIME_FILTER_MIN_PROBE_DATA_SIZE,
                            MemorySize.parse("2g"))
                    // BatchExecMultipleInput is not serializable into a compiled plan yet
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, false)
                    .setupTableSource(
                            SourceTestStep.newBuilder("fact_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .producedBeforeRestore(
                                            Row.of(1, 10L),
                                            Row.of(2, 20L),
                                            Row.of(3, 30L),
                                            Row.of(4, 40L),
                                            Row.of(5, 50L))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("dim_t")
                                    .addSchema("x INT", "y BIGINT")
                                    .producedBeforeRestore(
                                            Row.of(2, 1L),
                                            Row.of(3, 2L),
                                            Row.of(3, 3L),
                                            Row.of(9, 4L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b BIGINT", "s BIGINT")
                                    .consumedBeforeRestore("+I[2, 20, 1]", "+I[3, 30, 5]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a, b, s FROM fact_t JOIN "
                                    + "(SELECT x, SUM(y) AS s FROM dim_t WHERE y < 4 GROUP BY x) "
                                    + "ON a = x")
                    .build();
}
