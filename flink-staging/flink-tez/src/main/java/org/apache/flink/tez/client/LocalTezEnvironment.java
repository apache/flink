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

package org.apache.flink.tez.client;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

public class LocalTezEnvironment extends ExecutionEnvironment {

	TezExecutor executor;
	Optimizer compiler;

	private LocalTezEnvironment() {
		compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), new Configuration());
		executor = new TezExecutor(compiler, this.getParallelism());
	}

	public static LocalTezEnvironment create() {
		return new LocalTezEnvironment();
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		TezConfiguration tezConf = new TezConfiguration();
		tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
		tezConf.set("fs.defaultFS", "file:///");
		tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
		executor.setConfiguration(tezConf);
		return executor.executePlan(createProgramPlan(jobName));
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan(null, false);
		return executor.getOptimizerPlanAsJSON(p);
	}

	public void setAsContext() {
		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				return LocalTezEnvironment.this;
			}
		};
		initializeContextEnvironment(factory);
	}

	@Override
	public void startNewSession() throws Exception {
		throw new UnsupportedOperationException("Session management is not implemented in Flink on Tez.");
	}
}
