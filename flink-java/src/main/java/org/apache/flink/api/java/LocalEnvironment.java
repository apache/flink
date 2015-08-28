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

package org.apache.flink.api.java;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.configuration.Configuration;

/**
 * An {@link ExecutionEnvironment} that runs the program locally, multi-threaded, in the JVM where the
 * environment is instantiated. When this environment is instantiated, it uses a default parallelism
 * of {@code 1}. Local environments can also be instantiated through
 * {@link ExecutionEnvironment#createLocalEnvironment()} and {@link ExecutionEnvironment#createLocalEnvironment(int)}.
 * The former version will pick a default parallelism equal to the number of hardware contexts in the local
 * machine.
 */
public class LocalEnvironment extends ExecutionEnvironment {
	private Configuration configuration;
	/**
	 * Creates a new local environment.
	 */
	public LocalEnvironment() {
		if(!ExecutionEnvironment.localExecutionIsAllowed()) {
			throw new InvalidProgramException("The LocalEnvironment cannot be used when submitting a program through a client.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);
		
		PlanExecutor executor = PlanExecutor.createLocalExecutor(configuration);
		executor.setPrintStatusDuringExecution(p.getExecutionConfig().isSysoutLoggingEnabled());
		this.lastJobExecutionResult = executor.executePlan(p);
		return this.lastJobExecutionResult;
	}
	
	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan(null, false);
		
		PlanExecutor executor = PlanExecutor.createLocalExecutor(configuration);
		return executor.getOptimizerPlanAsJSON(p);
	}
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "Local Environment (parallelism = " + (getParallelism() == -1 ? "default" : getParallelism())
				+ ") : " + getIdString();
	}

	public void setConfiguration(Configuration customConfiguration) {
		this.configuration = customConfiguration;
	}
}
