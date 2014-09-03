/**
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
import org.apache.flink.util.LogUtils;
import org.apache.log4j.Level;


/**
 * An {@link ExecutionEnvironment} that runs the program locally, multi-threaded, in the JVM where the
 * environment is instantiated. When this environment is instantiated, it uses a default degree of parallelism
 * of {@code 1}. Local environments can also be instantiated through
 * {@link ExecutionEnvironment#createLocalEnvironment()} and {@link ExecutionEnvironment#createLocalEnvironment(int)}.
 * The former version will pick a default degree of parallelism equal to the number of hardware contexts in the local
 * machine.
 */
public class LocalEnvironment extends ExecutionEnvironment {
	
	private boolean logging = false;

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
		
		PlanExecutor executor = PlanExecutor.createLocalExecutor();
		initLogging();
		return executor.executePlan(p);
	}
	
	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan();
		
		PlanExecutor executor = PlanExecutor.createLocalExecutor();
		initLogging();
		return executor.getOptimizerPlanAsJSON(p);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Causes the local environment to print INFO level log messages to the standard error output.
	 */
	public void enableLogging() {
		this.logging = true;
	}
	
	/**
	 * Completely disables logging during the execution of programs in the local environment.
	 */
	public void disableLogging() {
		this.logging = false;
	}

	/**
	 * Checks whether logging during the program execution is enabled or disabled.
	 * <p>
	 * By default, logging is turned off.
	 * 
	 * @return True, if logging is enabled, false otherwise.
	 */
	public boolean isLoggingEnabled() {
		return this.logging;
	}
	
	private void initLogging() {
		LogUtils.initializeDefaultConsoleLogger(logging ? Level.INFO : Level.OFF);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "Local Environment (DOP = " + (getDegreeOfParallelism() == -1 ? "default" : getDegreeOfParallelism())
				+ ") : " + getIdString();
	}
}
