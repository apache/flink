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

package org.apache.flink.optimizer;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.testfunctions.SelectOneReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.io.network.DataExchangeMode;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests in this class validate that the {@link ExecutionMode#PIPELINED} execution mode properly
 * sets batch data exchanges, to guard against deadlocks, but does not place pipeline breakers.
 */
@SuppressWarnings("serial")
public class PipelineBreakerTest extends CompilerTestBase {

    @Test
    public void testPipelineBreakerWithBroadcastVariable() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
            env.setParallelism(64);

            DataSet<Long> source = env.generateSequence(1, 10).map(new IdentityMapper<Long>());

            DataSet<Long> result =
                    source.map(new IdentityMapper<Long>())
                            .map(new IdentityMapper<Long>())
                            .withBroadcastSet(source, "bc");

            result.output(new DiscardingOutputFormat<Long>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
            SingleInputPlanNode mapperInput = (SingleInputPlanNode) mapper.getInput().getSource();

            assertEquals(TempMode.NONE, mapper.getInput().getTempMode());
            assertEquals(TempMode.NONE, mapper.getBroadcastInputs().get(0).getTempMode());

            assertEquals(DataExchangeMode.BATCH, mapperInput.getInput().getDataExchangeMode());
            assertEquals(
                    DataExchangeMode.BATCH,
                    mapper.getBroadcastInputs().get(0).getDataExchangeMode());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPipelineBreakerBroadcastedAllReduce() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
            env.setParallelism(64);

            DataSet<Long> sourceWithMapper =
                    env.generateSequence(1, 10).map(new IdentityMapper<Long>());

            DataSet<Long> bcInput1 =
                    sourceWithMapper
                            .map(new IdentityMapper<Long>())
                            .reduce(new SelectOneReducer<Long>());
            DataSet<Long> bcInput2 = env.generateSequence(1, 10);

            DataSet<Long> result =
                    sourceWithMapper
                            .map(new IdentityMapper<Long>())
                            .withBroadcastSet(bcInput1, "bc1")
                            .withBroadcastSet(bcInput2, "bc2");

            result.output(new DiscardingOutputFormat<Long>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();

            assertEquals(TempMode.NONE, mapper.getInput().getTempMode());
            assertEquals(DataExchangeMode.BATCH, mapper.getInput().getDataExchangeMode());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     *
     *
     * <pre>
     *                                +----------- ITERATION ---------+
     *                                |                               |
     *                               +--+                           +----+
     *  (source 1) ----------------->|PS| ------------ +        +-->|next|---> (sink)
     *                               +--+              | (BC)   |   +----+
     *                                |                V        |     |
     *  (source 2) --> (map) --+------|-----------> (MAPPER) ---+     |
     *                         |      |                ^              |
     *                         |      |                | (BC)         |
     *                         |      +----------------|--------------+
     *                         |                       |
     *                         +--(map) --> (reduce) --+
     * </pre>
     */
    @Test
    public void testPipelineBreakerBroadcastedPartialSolution() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
            env.setParallelism(64);

            DataSet<Long> initialSource = env.generateSequence(1, 10);
            IterativeDataSet<Long> iteration = initialSource.iterate(100);

            DataSet<Long> sourceWithMapper =
                    env.generateSequence(1, 10).map(new IdentityMapper<Long>());

            DataSet<Long> bcInput1 =
                    sourceWithMapper
                            .map(new IdentityMapper<Long>())
                            .reduce(new SelectOneReducer<Long>());

            DataSet<Long> result =
                    sourceWithMapper
                            .map(new IdentityMapper<Long>())
                            .withBroadcastSet(iteration, "bc2")
                            .withBroadcastSet(bcInput1, "bc1");

            iteration.closeWith(result).output(new DiscardingOutputFormat<Long>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            BulkIterationPlanNode iterationPlanNode =
                    (BulkIterationPlanNode) sink.getInput().getSource();
            SingleInputPlanNode mapper =
                    (SingleInputPlanNode) iterationPlanNode.getRootOfStepFunction();

            assertEquals(TempMode.CACHED, mapper.getInput().getTempMode());
            assertEquals(DataExchangeMode.BATCH, mapper.getInput().getDataExchangeMode());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPipelineBreakerWithCross() {
        try {
            {
                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(64);

                DataSet<Long> initialSource = env.generateSequence(1, 10);

                Configuration conf = new Configuration();
                conf.setString(
                        Optimizer.HINT_LOCAL_STRATEGY,
                        Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST);
                initialSource
                        .map(new IdentityMapper<Long>())
                        .cross(initialSource)
                        .withParameters(conf)
                        .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

                Plan p = env.createProgramPlan();
                OptimizedPlan op = compileNoStats(p);
                SinkPlanNode sink = op.getDataSinks().iterator().next();
                DualInputPlanNode cross = (DualInputPlanNode) sink.getInput().getSource();
                SingleInputPlanNode mapper = (SingleInputPlanNode) cross.getInput1().getSource();

                assertEquals(TempMode.NONE, mapper.getInput().getTempMode());
                assertEquals(TempMode.NONE, cross.getInput2().getTempMode());
                assertEquals(DataExchangeMode.BATCH, mapper.getInput().getDataExchangeMode());
                assertEquals(DataExchangeMode.BATCH, cross.getInput2().getDataExchangeMode());
            }

            {
                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(64);

                DataSet<Long> initialSource = env.generateSequence(1, 10);

                Configuration conf = new Configuration();
                conf.setString(
                        Optimizer.HINT_LOCAL_STRATEGY,
                        Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND);
                initialSource
                        .map(new IdentityMapper<Long>())
                        .cross(initialSource)
                        .withParameters(conf)
                        .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

                Plan p = env.createProgramPlan();
                OptimizedPlan op = compileNoStats(p);

                SinkPlanNode sink = op.getDataSinks().iterator().next();
                DualInputPlanNode cross = (DualInputPlanNode) sink.getInput().getSource();
                SingleInputPlanNode mapper = (SingleInputPlanNode) cross.getInput1().getSource();

                assertEquals(TempMode.NONE, mapper.getInput().getTempMode());
                assertEquals(TempMode.NONE, cross.getInput2().getTempMode());
                assertEquals(DataExchangeMode.BATCH, mapper.getInput().getDataExchangeMode());
                assertEquals(DataExchangeMode.BATCH, cross.getInput2().getDataExchangeMode());
            }

            {
                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(64);

                DataSet<Long> initialSource = env.generateSequence(1, 10);

                Configuration conf = new Configuration();
                conf.setString(
                        Optimizer.HINT_LOCAL_STRATEGY,
                        Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST);
                initialSource
                        .map(new IdentityMapper<Long>())
                        .cross(initialSource)
                        .withParameters(conf)
                        .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

                Plan p = env.createProgramPlan();
                OptimizedPlan op = compileNoStats(p);

                SinkPlanNode sink = op.getDataSinks().iterator().next();
                DualInputPlanNode cross = (DualInputPlanNode) sink.getInput().getSource();
                SingleInputPlanNode mapper = (SingleInputPlanNode) cross.getInput1().getSource();

                assertEquals(TempMode.NONE, mapper.getInput().getTempMode());
                assertEquals(TempMode.NONE, cross.getInput2().getTempMode());
                assertEquals(DataExchangeMode.BATCH, mapper.getInput().getDataExchangeMode());
                assertEquals(DataExchangeMode.BATCH, cross.getInput2().getDataExchangeMode());
            }

            {
                ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(64);

                DataSet<Long> initialSource = env.generateSequence(1, 10);

                Configuration conf = new Configuration();
                conf.setString(
                        Optimizer.HINT_LOCAL_STRATEGY,
                        Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND);
                initialSource
                        .map(new IdentityMapper<Long>())
                        .cross(initialSource)
                        .withParameters(conf)
                        .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

                Plan p = env.createProgramPlan();
                OptimizedPlan op = compileNoStats(p);

                SinkPlanNode sink = op.getDataSinks().iterator().next();
                DualInputPlanNode cross = (DualInputPlanNode) sink.getInput().getSource();
                SingleInputPlanNode mapper = (SingleInputPlanNode) cross.getInput1().getSource();

                assertEquals(TempMode.NONE, mapper.getInput().getTempMode());
                assertEquals(TempMode.NONE, cross.getInput2().getTempMode());
                assertEquals(DataExchangeMode.BATCH, mapper.getInput().getDataExchangeMode());
                assertEquals(DataExchangeMode.BATCH, cross.getInput2().getDataExchangeMode());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
