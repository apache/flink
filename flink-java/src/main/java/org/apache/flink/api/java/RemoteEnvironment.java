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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * An {@link ExecutionEnvironment} that sends programs 
 * to a cluster for execution. Note that all file paths used in the program must be accessible from the
 * cluster. The execution will use the cluster's default parallelism, unless the parallelism is
 * set explicitly via {@link ExecutionEnvironment#setParallelism(int)}.
 */
public class RemoteEnvironment extends ExecutionEnvironment {
	
	private final String host;

	private final int port;

	private final String[] jarFiles;

	private static String clientClassName = "org.apache.flink.client.program.Client";

	private Thread shutdownHook;

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 * 
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed. 
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */	
	public RemoteEnvironment(String host, int port, String... jarFiles) {
		if (host == null) {
			throw new NullPointerException("Host must not be null.");
		}

		if (port < 1 || port >= 0xffff) {
			throw new IllegalArgumentException("Port out of range");
		}

		this.host = host;
		this.port = port;
		this.jarFiles = jarFiles;

		shutdownHook = new Thread() {
			@Override
			public void run() {
				endSession();
			}
		};

		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}


	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);

		PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, jarFiles);
		executor.setJobID(jobID);
		executor.setSessionTimeout(sessionTimeout);
		executor.setPrintStatusDuringExecution(p.getExecutionConfig().isSysoutLoggingEnabled());

		this.lastJobExecutionResult = executor.executePlan(p);
		return this.lastJobExecutionResult;
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan("unnamed", false);
		p.setDefaultParallelism(getParallelism());
		registerCachedFilesWithPlan(p);

		PlanExecutor executor = PlanExecutor.createRemoteExecutor(host, port, jarFiles);
		return executor.getOptimizerPlanAsJSON(p);
	}

	private void endSession() {
		System.out.println("Hello");
		try {
			Class<?> clientClass = Class.forName(clientClassName);
			Constructor<?> constructor = clientClass.getConstructor(Configuration.class, ClassLoader.class);
			Configuration configuration = new Configuration();
			configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, host);
			configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, port);
			Method setJobID = clientClass.getDeclaredMethod("setJobID", JobID.class);
			Method endSession = clientClass.getDeclaredMethod("endSession");

			Object client = constructor.newInstance(configuration, ClassLoader.getSystemClassLoader());
			setJobID.invoke(client, jobID);
			endSession.invoke(client);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("Couldn't find constructor/method method to invoke on the Client class.");
		} catch (InstantiationException e) {
			throw new RuntimeException("Couldn't instantiate the Client class.");
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Couldn't access the Client class or its methods.");
		} catch (InvocationTargetException e) {
			throw new RuntimeException("Couldn't invoke the Client class method.");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't find the Client class.");
		}
	}

	@Override
	public void startNewSession() {
		endSession();
		jobID = JobID.generate();
	}

	@Override
	public String toString() {
		return "Remote Environment (" + this.host + ":" + this.port + " - parallelism = " +
				(getParallelism() == -1 ? "default" : getParallelism()) + ") : " + getIdString();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		Runtime.getRuntime().removeShutdownHook(shutdownHook);
		endSession();
	}
}
