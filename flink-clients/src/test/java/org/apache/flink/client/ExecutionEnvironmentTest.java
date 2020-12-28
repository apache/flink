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
 *
 */

package org.apache.flink.client;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.Serializable;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ExecutionEnvironment}.
 *
 * <p>NOTE: This test is in the flink-client package because we cannot have it in flink-java, where
 * the JSON plan generator is not available. Making it available, by depending on flink-optimizer
 * would create a cyclic dependency.
 */
public class ExecutionEnvironmentTest extends TestLogger implements Serializable {

    /**
     * Tests that verifies consecutive calls to {@link ExecutionEnvironment#getExecutionPlan()} do
     * not cause any exceptions. {@link ExecutionEnvironment#getExecutionPlan()} must not modify the
     * state of the plan
     */
    @Test
    public void testExecuteAfterGetExecutionPlanContextEnvironment() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> baseSet = env.fromElements(1, 2);

        DataSet<Integer> result = baseSet.map((MapFunction<Integer, Integer>) value -> value * 2);
        result.output(new DiscardingOutputFormat<>());

        try {
            env.getExecutionPlan();
            env.getExecutionPlan();
        } catch (Exception e) {
            fail("Consecutive #getExecutionPlan calls caused an exception.");
        }
    }

    @Test
    public void testDefaultJobName() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        testJobName("Flink Java Job at", env);
    }

    @Test
    public void testUserDefinedJobName() {
        String jobName = "MyTestJob";
        Configuration config = new Configuration();
        config.set(PipelineOptions.NAME, jobName);
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(config);
        testJobName(jobName, env);
    }

    @Test
    public void testUserDefinedJobNameWithConfigure() {
        String jobName = "MyTestJob";
        Configuration config = new Configuration();
        config.set(PipelineOptions.NAME, jobName);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, this.getClass().getClassLoader());
        testJobName(jobName, env);
    }

    private void testJobName(String prefixOfExpectedJobName, ExecutionEnvironment env) {
        env.fromElements(1, 2, 3).writeAsText("/dev/null");
        Plan plan = env.createProgramPlan();
        assertTrue(plan.getJobName().startsWith(prefixOfExpectedJobName));
    }
}
