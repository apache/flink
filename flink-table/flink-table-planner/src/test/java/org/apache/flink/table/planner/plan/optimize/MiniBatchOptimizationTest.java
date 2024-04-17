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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;

/**
 * Test for enabling/disabling mini-batch assigner operator based on query plan. The optimization is
 * performed in {@link StreamCommonSubGraphBasedOptimizer}.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class MiniBatchOptimizationTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @Parameter public boolean isMiniBatchEnabled;

    public long miniBatchLatency;
    public long miniBatchSize;

    @BeforeEach
    public void setup() {
        miniBatchLatency = 5L;
        miniBatchSize = 10L;
        util.tableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, isMiniBatchEnabled)
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                        Duration.ofSeconds(miniBatchLatency))
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, miniBatchSize);
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTableA (\n"
                                + "  a BIGINT,\n"
                                + "  b INT NOT NULL,\n"
                                + "  c VARCHAR,\n"
                                + "  d BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTableB (\n"
                                + "  a BIGINT,\n"
                                + "  b INT NOT NULL,\n"
                                + "  c VARCHAR,\n"
                                + "  d BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false')");
    }

    @TestTemplate
    public void testMiniBatchWithAggregation() {
        final String aggQuery =
                "SELECT\n"
                        + "  AVG(a) AS avg_a,\n"
                        + "  COUNT(*) AS cnt,\n"
                        + "  count(b) AS cnt_b,\n"
                        + "  min(b) AS min_b,\n"
                        + "  MAX(c) FILTER (WHERE a > 1) AS max_c\n"
                        + "FROM MyTableA";

        if (isMiniBatchEnabled) {
            util.verifyRelPlanExpected(
                    aggQuery,
                    JavaScalaConversionUtil.toScala(
                            Collections.singletonList("MiniBatchAssigner")));
        } else {
            util.verifyRelPlanNotExpected(
                    aggQuery,
                    JavaScalaConversionUtil.toScala(
                            Collections.singletonList("MiniBatchAssigner")));
        }
    }

    @TestTemplate
    public void testMiniBatchWithJoin() {
        final String joinQuery = "SELECT * FROM MyTableA a, MyTableB b WHERE a.a = b.a";

        if (isMiniBatchEnabled) {
            util.verifyRelPlanExpected(
                    joinQuery,
                    JavaScalaConversionUtil.toScala(
                            Collections.singletonList("MiniBatchAssigner")));
        } else {
            util.verifyRelPlanNotExpected(
                    joinQuery,
                    JavaScalaConversionUtil.toScala(
                            Collections.singletonList("MiniBatchAssigner")));
        }
    }

    @TestTemplate
    public void testMiniBatchWithProjectFilter() {
        final String joinQuery = "SELECT b FROM MyTableA a WHERE a.a > 123";
        util.verifyRelPlanNotExpected(
                joinQuery,
                JavaScalaConversionUtil.toScala(Collections.singletonList("MiniBatchAssigner")));
    }

    @Parameters(name = "isMiniBatchEnabled={0}")
    public static Object[][] data() {
        return new Object[][] {new Object[] {true}, new Object[] {false}};
    }
}
