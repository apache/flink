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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * A PlanExecutor executes a Flink program's dataflow plan. All Flink programs are translated to
 * dataflow plans prior to execution.
 * 
 * <p>The specific implementation (such as the org.apache.flink.client.LocalExecutor
 * and org.apache.flink.client.RemoteExecutor) determines where and how to run the dataflow.
 * The concrete implementations of the executors are loaded dynamically, because they depend on
 * the full set of all runtime classes.</p>
 * 
 * <p>PlanExecutors can be started explicitly, in which case they keep running until stopped. If
 * a program is submitted to a plan executor that is not running, it will start up for that
 * program, and shut down afterwards.</p>
 */
@Internal
public abstract class PlanExecutor {

	private static final String LOCAL_EXECUTOR_CLASS = "org.apache.flink.client.LocalExecutor";
	private static final String REMOTE_EXECUTOR_CLASS = "org.apache.flink.client.RemoteExecutor";

	// ------------------------------------------------------------------------
	//  Config Options
	// ------------------------------------------------------------------------
	
	/** If true, all execution progress updates are not only logged, but also printed to System.out */
	private boolean printUpdatesToSysout = true;

	/**
	 * Sets whether the executor should print progress results to "standard out" ({@link System#out}).
	 * All progress messages are logged using the configured logging framework independent of the value
	 * set here.
	 * 
	 * @param printStatus True, to print progress updates to standard out, false to not do that. 
	 */
	public void setPrintStatusDuringExecution(boolean printStatus) {
		this.printUpdatesToSysout = printStatus;
	}

	/**
	 * Gets whether the executor prints progress results to "standard out" ({@link System#out}).
	 * 
	 * @return True, if the executor prints progress messages to standard out, false if not.
	 */
	public boolean isPrintingStatusDuringExecution() {
		return this.printUpdatesToSysout;
	}

	// ------------------------------------------------------------------------
	//  Startup & Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Starts the program executor. After the executor has been started, it will keep
	 * running until {@link #stop()} is called. 
	 * 
	 * @throws Exception Thrown, if the executor startup failed.
	 */
	public abstract void start() throws Exception;

	/**
	 * Shuts down the plan executor and releases all local resources.
	 *
	 * <p>This method also ends all sessions created by this executor. Remote job executions
	 * may complete, but the session is not kept alive after that.</p>
	 *
	 * @throws Exception Thrown, if the proper shutdown failed. 
	 */
	public abstract void stop() throws Exception;

	/**
	 * Checks if this executor is currently running.
	 * 
	 * @return True is the executor is running, false otherwise.
	 */
	public abstract boolean isRunning();
	
	// ------------------------------------------------------------------------
	//  Program Execution
	// ------------------------------------------------------------------------
	
	/**
	 * Execute the given program.
	 * 
	 * <p>If the executor has not been started before, then this method will start the
	 * executor and stop it after the execution has completed. This implies that one needs
	 * to explicitly start the executor for all programs where multiple dataflow parts
	 * depend on each other. Otherwise, the previous parts will no longer
	 * be available, because the executor immediately shut down after the execution.</p>
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
	 * @param globalClasspaths A list of URLs that are added to the classpath of each user code classloader of the
	 *                 program. Paths must specify a protocol (e.g. file://) and be accessible on all nodes.
	 * @return A remote executor.
	 */
	public static PlanExecutor createRemoteExecutor(String hostname, int port, Configuration clientConfiguration,
			List<URL> jarFiles, List<URL> globalClasspaths) {
		if (hostname == null) {
			throw new IllegalArgumentException("The hostname must not be null.");
		}
		if (port <= 0 || port > 0xffff) {
			throw new IllegalArgumentException("The port value is out of range.");
		}
		
		Class<? extends PlanExecutor> reClass = loadExecutorClass(REMOTE_EXECUTOR_CLASS);
		
		List<URL> files = (jarFiles == null) ?
				Collections.<URL>emptyList() : jarFiles;
		List<URL> paths = (globalClasspaths == null) ?
				Collections.<URL>emptyList() : globalClasspaths;

		try {
			PlanExecutor executor = (clientConfiguration == null) ?
					reClass.getConstructor(String.class, int.class, List.class)
						.newInstance(hostname, port, files) :
					reClass.getConstructor(String.class, int.class, Configuration.class, List.class, List.class)
						.newInstance(hostname, port, clientConfiguration, files, paths);
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
