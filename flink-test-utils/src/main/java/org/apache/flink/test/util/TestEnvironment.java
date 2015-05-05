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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.junit.Assert;

public class TestEnvironment extends ExecutionEnvironment {

	private final ForkableFlinkMiniCluster executor;

	public TestEnvironment(ForkableFlinkMiniCluster executor, int parallelism) {
		this.executor = executor;
		setParallelism(parallelism);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		try {
			OptimizedPlan op = compileProgram(jobName);

			JobGraphGenerator jgg = new JobGraphGenerator();
			JobGraph jobGraph = jgg.compileJobGraph(op);
			
			SerializedJobExecutionResult result = executor.submitJobAndWait(jobGraph, false);

			this.lastJobExecutionResult = result.toJobExecutionResult(getClass().getClassLoader());
			return this.lastJobExecutionResult;
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Job execution failed!");
			return null;
		}
	}


	@Override
	public String getExecutionPlan() throws Exception {
		OptimizedPlan op = compileProgram("unused");

		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON(op);
	}


	private OptimizedPlan compileProgram(String jobName) {
		Plan p = createProgramPlan(jobName);

		Optimizer pc = new Optimizer(new DataStatistics(), this.executor.getConfiguration());
		return pc.compile(p);
	}

	protected void setAsContext() {
		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				return TestEnvironment.this;
			}
		};

		initializeContextEnvironment(factory);
	}
}
