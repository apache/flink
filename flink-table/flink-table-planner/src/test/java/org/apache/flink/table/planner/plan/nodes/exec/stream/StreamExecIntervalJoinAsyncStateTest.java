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

import org.apache.flink.runtime.asyncprocessing.operators.co.AsyncKeyedCoProcessOperator;
import org.apache.flink.runtime.asyncprocessing.operators.co.AsyncKeyedCoProcessOperatorWithWatermarkDelay;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.CompiledPlanUtils;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.runtime.operators.join.KeyedCoProcessOperatorWithWatermarkDelay;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the operator selection of {@link StreamExecIntervalJoin}. */
class StreamExecIntervalJoinAsyncStateTest {

    private static final String PROC_TIME_JOIN =
            "INSERT INTO snk SELECT l.a, l.b, r.b FROM l JOIN r ON l.a = r.a"
                    + " AND l.pt BETWEEN r.pt - INTERVAL '5' SECOND AND r.pt + INTERVAL '5' SECOND";

    private static final String ROW_TIME_JOIN =
            "INSERT INTO snk SELECT l.a, l.b, r.b FROM l JOIN r ON l.a = r.a"
                    + " AND l.rt BETWEEN r.rt - INTERVAL '5' SECOND AND r.rt + INTERVAL '5' SECOND";

    @ParameterizedTest(name = "asyncState={0}")
    @ValueSource(booleans = {false, true})
    void testProcTimeJoin(boolean asyncState) {
        assertThat(translate(createTableEnvironment(asyncState), PROC_TIME_JOIN))
                .isExactlyInstanceOf(
                        asyncState
                                ? AsyncKeyedCoProcessOperator.class
                                : KeyedCoProcessOperator.class);
    }

    @ParameterizedTest(name = "asyncState={0}")
    @ValueSource(booleans = {false, true})
    void testRowTimeJoin(boolean asyncState) {
        assertThat(translate(createTableEnvironment(asyncState), ROW_TIME_JOIN))
                .isExactlyInstanceOf(
                        asyncState
                                ? AsyncKeyedCoProcessOperatorWithWatermarkDelay.class
                                : KeyedCoProcessOperatorWithWatermarkDelay.class);
    }

    @Test
    void testEarlyFireJoinKeepsSyncState() {
        String sql =
                "INSERT INTO snk SELECT /*+ EARLY_FIRE('delay'='1s') */ l.a, l.b, r.b"
                        + " FROM l LEFT JOIN r ON l.a = r.a"
                        + " AND l.rt BETWEEN r.rt - INTERVAL '5' SECOND AND r.rt + INTERVAL '5' SECOND";
        assertThat(translate(createTableEnvironment(true), sql))
                .isExactlyInstanceOf(KeyedCoProcessOperatorWithWatermarkDelay.class);
    }

    @Test
    void testPlanDoesNotDependOnAsyncState() {
        ExecNodeContext.resetIdCounter();
        String json = createTableEnvironment(false).compilePlanSql(ROW_TIME_JOIN).asJsonString();
        TableEnvironment tEnv = createTableEnvironment(true);
        ExecNodeContext.resetIdCounter();
        assertThat(tEnv.compilePlanSql(ROW_TIME_JOIN).asJsonString()).isEqualTo(json);

        // A plan compiled without async state runs with it once it is enabled.
        assertThat(translate(tEnv, tEnv.loadPlan(PlanReference.fromJsonString(json))))
                .isExactlyInstanceOf(AsyncKeyedCoProcessOperatorWithWatermarkDelay.class);
    }

    private static TableEnvironment createTableEnvironment(boolean asyncState) {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, asyncState);
        for (String table : new String[] {"l", "r"}) {
            tEnv.executeSql(
                    "CREATE TABLE "
                            + table
                            + " (a INT, b STRING, pt AS PROCTIME(), rt TIMESTAMP(3),"
                            + " WATERMARK FOR rt AS rt) WITH ('connector' = 'values')");
        }
        tEnv.executeSql(
                "CREATE TABLE snk (a INT, b STRING, c STRING)"
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')");
        return tEnv;
    }

    private static StreamOperator<?> translate(TableEnvironment tEnv, String sql) {
        return translate(tEnv, tEnv.compilePlanSql(sql));
    }

    private static StreamOperator<?> translate(TableEnvironment tEnv, CompiledPlan plan) {
        ExecNode<?> join = CompiledPlanUtils.unwrap(plan).getExecNodeGraph().getRootNodes().get(0);
        while (!(join instanceof StreamExecIntervalJoin)) {
            join = join.getInputEdges().get(0).getSource();
        }
        CompiledPlanUtils.toTransformations(tEnv, plan);
        // the translation of each node is cached
        return ((TwoInputTransformation<?, ?, ?>)
                        join.translateToPlan(((TableEnvironmentImpl) tEnv).getPlanner()))
                .getOperator();
    }
}
