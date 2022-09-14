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

package org.apache.flink.optimizer.java;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class IterationCompilerTest extends CompilerTestBase {

    @Test
    public void testIdentityIteration() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(43);

            IterativeDataSet<Long> iteration = env.generateSequence(-4, 1000).iterate(100);
            iteration.closeWith(iteration).output(new DiscardingOutputFormat<Long>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            new JobGraphGenerator().compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testEmptyWorksetIteration() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(43);

            DataSet<Tuple2<Long, Long>> input =
                    env.generateSequence(1, 20)
                            .map(
                                    new MapFunction<Long, Tuple2<Long, Long>>() {
                                        @Override
                                        public Tuple2<Long, Long> map(Long value) {
                                            return null;
                                        }
                                    });

            DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iter =
                    input.iterateDelta(input, 100, 0);
            iter.closeWith(iter.getWorkset(), iter.getWorkset())
                    .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            new JobGraphGenerator().compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIterationWithUnionRoot() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(43);

            IterativeDataSet<Long> iteration = env.generateSequence(-4, 1000).iterate(100);

            iteration
                    .closeWith(
                            iteration
                                    .map(new IdentityMapper<Long>())
                                    .union(iteration.map(new IdentityMapper<Long>())))
                    .output(new DiscardingOutputFormat<Long>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            BulkIterationPlanNode iterNode = (BulkIterationPlanNode) sink.getInput().getSource();

            // make sure that the root is part of the dynamic path

            // the "NoOp" that comes after the union.
            SingleInputPlanNode noop = (SingleInputPlanNode) iterNode.getRootOfStepFunction();
            NAryUnionPlanNode union = (NAryUnionPlanNode) noop.getInput().getSource();

            assertTrue(noop.isOnDynamicPath());
            assertTrue(noop.getCostWeight() >= 1);

            assertTrue(union.isOnDynamicPath());
            assertTrue(union.getCostWeight() >= 1);

            // see that the jobgraph generator can translate this
            new JobGraphGenerator().compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testWorksetIterationWithUnionRoot() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(43);

            DataSet<Tuple2<Long, Long>> input =
                    env.generateSequence(1, 20)
                            .map(
                                    new MapFunction<Long, Tuple2<Long, Long>>() {
                                        @Override
                                        public Tuple2<Long, Long> map(Long value) {
                                            return null;
                                        }
                                    });

            DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iter =
                    input.iterateDelta(input, 100, 0);
            iter.closeWith(
                            iter.getWorkset()
                                    .map(new IdentityMapper<Tuple2<Long, Long>>())
                                    .union(
                                            iter.getWorkset()
                                                    .map(new IdentityMapper<Tuple2<Long, Long>>())),
                            iter.getWorkset()
                                    .map(new IdentityMapper<Tuple2<Long, Long>>())
                                    .union(
                                            iter.getWorkset()
                                                    .map(new IdentityMapper<Tuple2<Long, Long>>())))
                    .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            SinkPlanNode sink = op.getDataSinks().iterator().next();
            WorksetIterationPlanNode iterNode =
                    (WorksetIterationPlanNode) sink.getInput().getSource();

            // make sure that the root is part of the dynamic path

            // the "NoOp"a that come after the union.
            SingleInputPlanNode nextWorksetNoop =
                    (SingleInputPlanNode) iterNode.getNextWorkSetPlanNode();
            SingleInputPlanNode solutionDeltaNoop =
                    (SingleInputPlanNode) iterNode.getSolutionSetDeltaPlanNode();

            NAryUnionPlanNode nextWorksetUnion =
                    (NAryUnionPlanNode) nextWorksetNoop.getInput().getSource();
            NAryUnionPlanNode solutionDeltaUnion =
                    (NAryUnionPlanNode) solutionDeltaNoop.getInput().getSource();

            assertTrue(nextWorksetNoop.isOnDynamicPath());
            assertTrue(nextWorksetNoop.getCostWeight() >= 1);

            assertTrue(solutionDeltaNoop.isOnDynamicPath());
            assertTrue(solutionDeltaNoop.getCostWeight() >= 1);

            assertTrue(nextWorksetUnion.isOnDynamicPath());
            assertTrue(nextWorksetUnion.getCostWeight() >= 1);

            assertTrue(solutionDeltaUnion.isOnDynamicPath());
            assertTrue(solutionDeltaUnion.getCostWeight() >= 1);

            new JobGraphGenerator().compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
