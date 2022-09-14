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

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.optimizer.plan.BinaryUnionPlanNode;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class UnionBetweenDynamicAndStaticPathTest extends CompilerTestBase {

    @Test
    public void testUnionStaticFirst() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Long> input1 = env.generateSequence(1, 10);
            DataSet<Long> input2 = env.generateSequence(1, 10);

            IterativeDataSet<Long> iteration = input1.iterate(10);

            DataSet<Long> result =
                    iteration.closeWith(input2.union(input2).union(iteration.union(iteration)));

            result.output(new DiscardingOutputFormat<Long>());
            result.output(new DiscardingOutputFormat<Long>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            assertEquals(2, op.getDataSinks().size());

            BulkIterationPlanNode iterPlan =
                    (BulkIterationPlanNode)
                            op.getDataSinks().iterator().next().getInput().getSource();

            SingleInputPlanNode noopNode = (SingleInputPlanNode) iterPlan.getRootOfStepFunction();
            BinaryUnionPlanNode mixedUnion = (BinaryUnionPlanNode) noopNode.getInput().getSource();
            NAryUnionPlanNode staticUnion = (NAryUnionPlanNode) mixedUnion.getInput1().getSource();
            NAryUnionPlanNode dynamicUnion = (NAryUnionPlanNode) mixedUnion.getInput2().getSource();

            assertTrue(mixedUnion.unionsStaticAndDynamicPath());
            assertFalse(mixedUnion.getInput1().isOnDynamicPath());
            assertTrue(mixedUnion.getInput2().isOnDynamicPath());
            assertTrue(mixedUnion.getInput1().getTempMode().isCached());

            for (Channel c : staticUnion.getInputs()) {
                assertFalse(c.isOnDynamicPath());
            }
            for (Channel c : dynamicUnion.getInputs()) {
                assertTrue(c.isOnDynamicPath());
            }

            assertEquals(0.5, iterPlan.getRelativeMemoryPerSubTask(), 0.0);
            assertEquals(0.5, mixedUnion.getInput1().getRelativeTempMemory(), 0.0);
            assertEquals(0.0, mixedUnion.getInput2().getRelativeTempMemory(), 0.0);

            new JobGraphGenerator().compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testUnionStaticSecond() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Long> input1 = env.generateSequence(1, 10);
            DataSet<Long> input2 = env.generateSequence(1, 10);

            IterativeDataSet<Long> iteration = input1.iterate(10);

            DataSet<Long> iterResult =
                    iteration.closeWith(iteration.union(iteration).union(input2.union(input2)));

            iterResult.output(new DiscardingOutputFormat<Long>());
            iterResult.output(new DiscardingOutputFormat<Long>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            assertEquals(2, op.getDataSinks().size());

            BulkIterationPlanNode iterPlan =
                    (BulkIterationPlanNode)
                            op.getDataSinks().iterator().next().getInput().getSource();

            SingleInputPlanNode noopNode = (SingleInputPlanNode) iterPlan.getRootOfStepFunction();
            BinaryUnionPlanNode mixedUnion = (BinaryUnionPlanNode) noopNode.getInput().getSource();
            NAryUnionPlanNode staticUnion = (NAryUnionPlanNode) mixedUnion.getInput1().getSource();
            NAryUnionPlanNode dynamicUnion = (NAryUnionPlanNode) mixedUnion.getInput2().getSource();

            assertTrue(mixedUnion.unionsStaticAndDynamicPath());
            assertFalse(mixedUnion.getInput1().isOnDynamicPath());
            assertTrue(mixedUnion.getInput2().isOnDynamicPath());
            assertTrue(mixedUnion.getInput1().getTempMode().isCached());

            assertEquals(0.5, iterPlan.getRelativeMemoryPerSubTask(), 0.0);
            assertEquals(0.5, mixedUnion.getInput1().getRelativeTempMemory(), 0.0);
            assertEquals(0.0, mixedUnion.getInput2().getRelativeTempMemory(), 0.0);

            for (Channel c : staticUnion.getInputs()) {
                assertFalse(c.isOnDynamicPath());
            }
            for (Channel c : dynamicUnion.getInputs()) {
                assertTrue(c.isOnDynamicPath());
            }

            new JobGraphGenerator().compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
