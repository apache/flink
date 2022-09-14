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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class WorksetIterationCornerCasesTest extends CompilerTestBase {

    @Test
    public void testWorksetIterationNotDependingOnSolutionSet() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<Long, Long>> input =
                    env.generateSequence(1, 100).map(new Duplicator<Long>());

            DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                    input.iterateDelta(input, 100, 1);

            DataSet<Tuple2<Long, Long>> iterEnd =
                    iteration.getWorkset().map(new TestMapper<Tuple2<Long, Long>>());
            iteration
                    .closeWith(iterEnd, iterEnd)
                    .output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            WorksetIterationPlanNode wipn =
                    (WorksetIterationPlanNode)
                            op.getDataSinks().iterator().next().getInput().getSource();
            assertTrue(wipn.getSolutionSetPlanNode().getOutgoingChannels().isEmpty());

            JobGraphGenerator jgg = new JobGraphGenerator();
            jgg.compileJobGraph(op);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private static final class Duplicator<T> implements MapFunction<T, Tuple2<T, T>> {
        @Override
        public Tuple2<T, T> map(T value) {
            return new Tuple2<T, T>(value, value);
        }
    }

    private static final class TestMapper<T> implements MapFunction<T, T> {
        @Override
        public T map(T value) {
            return value;
        }
    }
}
