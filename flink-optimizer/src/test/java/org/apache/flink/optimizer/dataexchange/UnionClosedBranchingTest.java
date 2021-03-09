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
package org.apache.flink.optimizer.dataexchange;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.runtime.io.network.DataExchangeMode.BATCH;
import static org.apache.flink.runtime.io.network.DataExchangeMode.PIPELINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This tests a fix for FLINK-2540.
 *
 * <p>This test is necessary, because {@link NAryUnionPlanNode}s are not directly translated to
 * runtime tasks by the {@link JobGraphGenerator}. Instead, the network stack unions the inputs by
 * directly reading from multiple inputs (via {@link UnionInputGate}).
 *
 * <pre>
 *   (source)-\        /-\
 *            (union)-+  (join)
 *   (source)-/        \-/
 * </pre>
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-2540">FLINK-2540</a>
 */
@RunWith(Parameterized.class)
@SuppressWarnings({"serial", "unchecked"})
public class UnionClosedBranchingTest extends CompilerTestBase {

    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        Collection<Object[]> params =
                Arrays.asList(
                        new Object[][] {
                            {ExecutionMode.PIPELINED, BATCH, PIPELINED},
                            {ExecutionMode.PIPELINED_FORCED, PIPELINED, PIPELINED},
                            {ExecutionMode.BATCH, BATCH, PIPELINED},
                            {ExecutionMode.BATCH_FORCED, BATCH, BATCH},
                        });

        // Make sure that changes to ExecutionMode are reflected in this test.
        assertEquals(ExecutionMode.values().length, params.size());

        return params;
    }

    private final ExecutionMode executionMode;

    /** Expected {@link DataExchangeMode} from sources to union. */
    private final DataExchangeMode sourceToUnion;

    /** Expected {@link DataExchangeMode} from union to join. */
    private final DataExchangeMode unionToJoin;

    /** Expected {@link ShipStrategyType} from source to union. */
    private final ShipStrategyType sourceToUnionStrategy = ShipStrategyType.PARTITION_HASH;

    /** Expected {@link ShipStrategyType} from union to join. */
    private final ShipStrategyType unionToJoinStrategy = ShipStrategyType.FORWARD;

    public UnionClosedBranchingTest(
            ExecutionMode executionMode,
            DataExchangeMode sourceToUnion,
            DataExchangeMode unionToJoin) {

        this.executionMode = executionMode;
        this.sourceToUnion = sourceToUnion;
        this.unionToJoin = unionToJoin;
    }

    @Test
    public void testUnionClosedBranchingTest() throws Exception {

        // -----------------------------------------------------------------------------------------
        // Build test program
        // -----------------------------------------------------------------------------------------

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setExecutionMode(executionMode);
        env.setParallelism(4);

        DataSet<Tuple1<Integer>> src1 = env.fromElements(new Tuple1<>(0), new Tuple1<>(1));

        DataSet<Tuple1<Integer>> src2 = env.fromElements(new Tuple1<>(0), new Tuple1<>(1));

        DataSet<Tuple1<Integer>> union = src1.union(src2);

        DataSet<Tuple2<Integer, Integer>> join =
                union.join(union).where(0).equalTo(0).projectFirst(0).projectSecond(0);

        join.output(new DiscardingOutputFormat<Tuple2<Integer, Integer>>());

        // -----------------------------------------------------------------------------------------
        // Verify optimized plan
        // -----------------------------------------------------------------------------------------

        OptimizedPlan optimizedPlan = compileNoStats(env.createProgramPlan());

        SinkPlanNode sinkNode = optimizedPlan.getDataSinks().iterator().next();

        DualInputPlanNode joinNode = (DualInputPlanNode) sinkNode.getPredecessor();

        // Verify that the compiler correctly sets the expected data exchange modes.
        for (Channel channel : joinNode.getInputs()) {
            assertEquals(
                    "Unexpected data exchange mode between union and join node.",
                    unionToJoin,
                    channel.getDataExchangeMode());
            assertEquals(
                    "Unexpected ship strategy between union and join node.",
                    unionToJoinStrategy,
                    channel.getShipStrategy());
        }

        for (SourcePlanNode src : optimizedPlan.getDataSources()) {
            for (Channel channel : src.getOutgoingChannels()) {
                assertEquals(
                        "Unexpected data exchange mode between source and union node.",
                        sourceToUnion,
                        channel.getDataExchangeMode());
                assertEquals(
                        "Unexpected ship strategy between source and union node.",
                        sourceToUnionStrategy,
                        channel.getShipStrategy());
            }
        }

        // -----------------------------------------------------------------------------------------
        // Verify generated JobGraph
        // -----------------------------------------------------------------------------------------

        JobGraphGenerator jgg = new JobGraphGenerator();
        JobGraph jobGraph = jgg.compileJobGraph(optimizedPlan);

        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        // Sanity check for the test setup
        assertEquals("Unexpected number of vertices created.", 4, vertices.size());

        // Verify all sources
        JobVertex[] sources = new JobVertex[] {vertices.get(0), vertices.get(1)};

        for (JobVertex src : sources) {
            // Sanity check
            assertTrue("Unexpected vertex type. Test setup is broken.", src.isInputVertex());

            // The union is not translated to an extra union task, but the join uses a union
            // input gate to read multiple inputs. The source create a single result per consumer.
            assertEquals(
                    "Unexpected number of created results.",
                    2,
                    src.getNumberOfProducedIntermediateDataSets());

            for (IntermediateDataSet dataSet : src.getProducedDataSets()) {
                ResultPartitionType dsType = dataSet.getResultType();

                // Ensure batch exchange unless PIPELINED_FORCE is enabled.
                if (!executionMode.equals(ExecutionMode.PIPELINED_FORCED)) {
                    assertTrue(
                            "Expected batch exchange, but result type is " + dsType + ".",
                            dsType.isBlocking());
                } else {
                    assertFalse(
                            "Expected non-batch exchange, but result type is " + dsType + ".",
                            dsType.isBlocking());
                }
            }
        }
    }
}
