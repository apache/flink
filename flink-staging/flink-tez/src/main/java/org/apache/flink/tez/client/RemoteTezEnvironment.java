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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.ToolRunner;


public class RemoteTezEnvironment extends ExecutionEnvironment {

	private static final Log LOG = LogFactory.getLog(RemoteTezEnvironment.class);
	
	private Optimizer compiler;
	private TezExecutor executor;
	private Path jarPath = null;
	

	public void registerMainClass (Class mainClass) {
		jarPath = new Path(ClassUtil.findContainingJar(mainClass));
		LOG.info ("Registering main class " + mainClass.getName() + " contained in " + jarPath.toString());
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		TezExecutorTool tool = new TezExecutorTool(executor, createProgramPlan());
		if (jarPath != null) {
			tool.setJobJar(jarPath);
		}
		try {
			int executionResult = ToolRunner.run(new Configuration(), tool, new String[]{jobName});
		}
		finally {
			return new JobExecutionResult(null, -1, null);
		}

	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan(null, false);
		return executor.getOptimizerPlanAsJSON(p);
	}

	public static RemoteTezEnvironment create () {
		return new RemoteTezEnvironment();
	}

	public RemoteTezEnvironment() {
		compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), new org.apache.flink.configuration.Configuration());
		executor = new TezExecutor(compiler, this.getDegreeOfParallelism());
	}

	@Override
	public void startNewSession() throws Exception {
		throw new UnsupportedOperationException("Session management is not implemented in Flink on Tez.");
	}
}
