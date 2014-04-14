/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java;

import org.apache.log4j.Level;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.PlanExecutor;
import eu.stratosphere.util.LogUtils;


public class LocalEnvironment extends ExecutionEnvironment {
	
	private boolean logging = false;

	private int numTaskManager = 1;
	
	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);
		p.setDefaultParallelism(getDegreeOfParallelism());
		registerCachedFiles(p);
		
		PlanExecutor executor = PlanExecutor.createLocalExecutor(numTaskManager);
		initLogging();
		return executor.executePlan(p);
	}
	
	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan("unnamed job");
		p.setDefaultParallelism(getDegreeOfParallelism());
		registerCachedFiles(p);
		
		PlanExecutor executor = PlanExecutor.createLocalExecutor(numTaskManager);
		initLogging();
		return executor.getOptimizerPlanAsJSON(p);
	}
	
	public void enableLogging() {
		this.logging = true;
	}
	
	public void disableLogging() {
		this.logging = false;
	}

	public void setNumTaskManager(int numTaskManager){
		this.numTaskManager = numTaskManager;
	}

	public int getNumTaskManager() { return this.numTaskManager; }
	
	public boolean isLoggingEnabled() {
		return this.logging;
	}
	
	private void initLogging() {
		LogUtils.initializeDefaultConsoleLogger(logging ? Level.INFO : Level.OFF);
	}

	@Override
	public String toString() {
		return "Local Environment (DOP = " + (getDegreeOfParallelism() == -1 ? "default" : getDegreeOfParallelism())
				+ "Number task manager = " + getNumTaskManager() + ") : " + getIdString();
	}
}
