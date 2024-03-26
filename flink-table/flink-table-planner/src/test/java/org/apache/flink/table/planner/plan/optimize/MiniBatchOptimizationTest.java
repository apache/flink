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

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTrait;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for enabling/disabling mini-batch assigner operator based on query plan. The optimization is
 * performed in {@link StreamCommonSubGraphBasedOptimizer}.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class MiniBatchOptimizationTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());
    private final StreamTableEnvironment streamTableEnv =
            StreamTableEnvironment.create(util.getStreamEnv());

    @Parameter public boolean isMiniBatchEnabled;

    @Parameter(1)
    public long miniBatchLatency;

    @Parameter(2)
    public long miniBatchSize;

    @BeforeEach
    public void setup() {
        streamTableEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, isMiniBatchEnabled)
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                        Duration.ofSeconds(miniBatchLatency))
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, miniBatchSize);
        streamTableEnv.executeSql(
                "CREATE TABLE MyTableA (\n"
                        + "  a BIGINT,\n"
                        + "  b INT NOT NULL,\n"
                        + "  c VARCHAR,\n"
                        + "  d BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')");
        streamTableEnv.executeSql(
                "CREATE TABLE MyTableB (\n"
                        + "  a BIGINT,\n"
                        + "  b INT NOT NULL,\n"
                        + "  c VARCHAR,\n"
                        + "  d BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')");
    }

    private boolean containsMiniBatch(String sql) {
        final Table result = streamTableEnv.sqlQuery(sql);
        RelNode relNode = TableTestUtil.toRelNode(result);
        StreamPlanner planner =
                (StreamPlanner) ((TableEnvironmentImpl) streamTableEnv).getPlanner();
        StreamCommonSubGraphBasedOptimizer optimizer =
                new StreamCommonSubGraphBasedOptimizer(planner);
        Seq<RelNode> nodeSeq =
                JavaConverters.asScalaIteratorConverter(Arrays.asList(relNode).iterator())
                        .asScala()
                        .toSeq();
        Seq<RelNodeBlock> blockSeq = optimizer.doOptimize(nodeSeq);
        List<RelNodeBlock> blockList = scala.collection.JavaConverters.seqAsJavaList(blockSeq);
        boolean res =
                blockList.stream()
                        .map(
                                b ->
                                        !b.getMiniBatchInterval()
                                                .equals(
                                                        MiniBatchIntervalTrait.NONE()
                                                                .getMiniBatchInterval()))
                        .reduce(false, (l, r) -> l || r);
        return res;
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

        boolean containsMiniBatch = containsMiniBatch(aggQuery);
        if (isMiniBatchEnabled) {
            assertTrue(containsMiniBatch);
        } else {
            assertFalse(containsMiniBatch);
        }
    }

    @TestTemplate
    public void testMiniBatchWithJoin() {
        final String joinQuery = "SELECT * FROM MyTableA a, MyTableB b WHERE a.a = b.a";

        boolean containsMiniBatch = containsMiniBatch(joinQuery);
        if (isMiniBatchEnabled) {
            assertTrue(containsMiniBatch);
        } else {
            assertFalse(containsMiniBatch);
        }
    }

    @TestTemplate
    public void testMiniBatchWithProjectFilter() {
        final String joinQuery = "SELECT b FROM MyTableA a WHERE a.a > 123";

        boolean containsMiniBatch = containsMiniBatch(joinQuery);
        assertFalse(containsMiniBatch);
    }

    @Parameters(name = "isMiniBatchEnabled={0}, miniBatchLatency={1}, miniBatchSize={2}")
    public static Object[][] data() {
        return new Object[][] {
            new Object[] {true, 10L, 5L},
            new Object[] {false, 10L, 5L}
        };
    }
}
