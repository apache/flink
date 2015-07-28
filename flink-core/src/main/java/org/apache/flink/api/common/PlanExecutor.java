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


package org.apache.flink.api.common;

import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A PlanExecutor runs a plan. The specific implementation (such as the org.apache.flink.client.LocalExecutor
 * and org.apache.flink.client.RemoteExecutor) determines where and how to run the plan.
 * 
 * The concrete implementations are loaded dynamically, because they depend on the full set of
 * dependencies of all runtime classes.
 */
public abstract class PlanExecutor {
	
	private static final String LOCAL_EXECUTOR_CLASS = "org.apache.flink.client.LocalExecutor";
	private static final String REMOTE_EXECUTOR_CLASS = "org.apache.flink.client.RemoteExecutor";

	// ------------------------------------------------------------------------
	//  Config Options
	// ------------------------------------------------------------------------
	
	/** If true, all execution progress updates are not only logged, but also printed to System.out */
	private boolean printUpdatesToSysout = true;
	
	public void setPrintStatusDuringExecution(boolean printStatus) {
		this.printUpdatesToSysout = printStatus;
	}
	
	public boolean isPrintingStatusDuringExecution() {
		return this.printUpdatesToSysout;
	}
	
	// ------------------------------------------------------------------------
	//  Program Execution
	// ------------------------------------------------------------------------
	
	/**
	 * Execute the given plan and return the runtime in milliseconds.
	 * 
	 * @param plan The plan of the program to execute.
	 * @return The execution result, containing for example the net runtime of the program, and the accumulators.
	 * 
	 * @throws Exception Thrown, if job submission caused an exception.
	 */
	public abstract JobExecutionResult executePlan(Plan plan) throws Exception;
	
	
	/**
	 * Gets the programs execution plan in a JSON format.
	 * 
	 * @param plan The program to get the execution plan for.
	 * @return The execution plan, as a JSON string.
	 * 
	 * @throws Exception Thrown, if the executor could not connect to the compiler.
	 */
	public abstract String getOptimizerPlanAsJSON(Plan plan) throws Exception;


	// ------------------------------------------------------------------------
	//  Executor Factories
	// ------------------------------------------------------------------------
	
	/**
	 * Creates an executor that runs the plan locally in a multi-threaded environment.
	 * 
	 * @return A local executor.
	 */
	public static PlanExecutor createLocalExecutor(Configuration configuration) {
		Class<? extends PlanExecutor> leClass = loadExecutorClass(LOCAL_EXECUTOR_CLASS);
		
		try {
			return leClass.getConstructor(Configuration.class).newInstance(configuration);
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the local executor ("
					+ LOCAL_EXECUTOR_CLASS + ").", t);
		}
	}

	/**
	 * Creates an executor that runs the plan on a remote environment. The remote executor is typically used
	 * to send the program to a cluster for execution.
	 * 
	 * @param hostname The address of the JobManager to send the program to.
	 * @param port The port of the JobManager to send the program to.
	 * @param clientConfiguration The configuration for the client (Akka, default.parallelism).
	 * @param jarFiles A list of jar files that contain the user-defined function (UDF) classes and all classes used
	 *                 from within the UDFs.
	 * @return A remote executor.
	 */
	public static PlanExecutor createRemoteExecutor(String hostname, int port, Configuration clientConfiguration, String... jarFiles) {
		if (hostname == null) {
			throw new IllegalArgumentException("The hostname must not be null.");
		}
		if (port <= 0 || port > 0xffff) {
			throw new IllegalArgumentException("The port value is out of range.");
		}
		
		Class<? extends PlanExecutor> reClass = loadExecutorClass(REMOTE_EXECUTOR_CLASS);
		
		List<String> files = (jarFiles == null || jarFiles.length == 0) ? Collections.<String>emptyList()
																		: Arrays.asList(jarFiles); 
		
		try {
			PlanExecutor executor = (clientConfiguration == null) ?
					reClass.getConstructor(String.class, int.class, List.class).newInstance(hostname, port, files) :
					reClass.getConstructor(String.class, int.class, List.class, Configuration.class).newInstance(hostname, port, files, clientConfiguration);
			return executor;
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the remote executor ("
					+ REMOTE_EXECUTOR_CLASS + ").", t);
		}
	}
	
	private static Class<? extends PlanExecutor> loadExecutorClass(String className) {
		try {
			Class<?> leClass = Class.forName(className);
			return leClass.asSubclass(PlanExecutor.class);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Could not load the executor class (" + className
					+ "). Do you have the 'flink-clients' project in your dependencies?");
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the executor (" + className + ").", t);
		}
	}
}
