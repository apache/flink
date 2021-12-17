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
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * This test case has been created to validate a bug that occurred when the ReduceOperator was used
 * without a grouping key.
 */
@SuppressWarnings({"serial"})
public class ReduceAllTest extends CompilerTestBase {

    @Test
    public void testReduce() {
        // construct the plan
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataSet<Long> set1 = env.generateSequence(0, 1);

        set1.reduceGroup(new IdentityGroupReducer<Long>())
                .name("Reduce1")
                .output(new DiscardingOutputFormat<Long>())
                .name("Sink");

        Plan plan = env.createProgramPlan();

        try {
            OptimizedPlan oPlan = compileNoStats(plan);
            JobGraphGenerator jobGen = new JobGraphGenerator();
            jobGen.compileJobGraph(oPlan);
        } catch (CompilerException ce) {
            ce.printStackTrace();
            fail("The pact compiler is unable to compile this plan correctly");
        }
    }
}
