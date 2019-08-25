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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ShutdownHookUtil;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An {@link ExecutionEnvironment} that sends programs to a cluster for execution. The environment
 * needs to be created with the address and port of the JobManager of the Flink cluster that
 * should execute the programs.
 *
 * <p>Many programs executed via the remote environment depend on additional classes. Such classes
 * may be the classes of functions (transformation, aggregation, ...) or libraries. Those classes
 * must be attached to the remote environment as JAR files, to allow the environment to ship the
 * classes into the cluster for the distributed execution.
 */
@Public
public class RemoteEnvironment extends ExecutionEnvironment {

	/** The hostname of the JobManager. */
	protected final String host;

	/** The port of the JobManager main actor system. */
	protected final int port;

	/** The jar files that need to be attached to each job. */
	protected final List<URL> jarFiles;

	/** The configuration used by the client that connects to the cluster. */
	protected Configuration clientConfiguration;

	/** The remote executor lazily created upon first use. */
	protected PlanExecutor executor;

	/** Optional shutdown hook, used in session mode to eagerly terminate the last session. */
	private Thread shutdownHook;

	/** The classpaths that need to be attached to each job. */
	protected final List<URL> globalClasspaths;

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */
	public RemoteEnvironment(String host, int port, String... jarFiles) {
		this(host, port, null, jarFiles, null);
	}

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param clientConfig The configuration used by the client that connects to the cluster.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */
	public RemoteEnvironment(String host, int port, Configuration clientConfig, String[] jarFiles) {
		this(host, port, clientConfig, jarFiles, null);
	}

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param clientConfig The configuration used by the client that connects to the cluster.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 * @param globalClasspaths The paths of directories and JAR files that are added to each user code
	 *                 classloader on all nodes in the cluster. Note that the paths must specify a
	 *                 protocol (e.g. file://) and be accessible on all nodes (e.g. by means of a NFS share).
	 *                 The protocol must be supported by the {@link java.net.URLClassLoader}.
	 */
	public RemoteEnvironment(String host, int port, Configuration clientConfig,
			String[] jarFiles, URL[] globalClasspaths) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The RemoteEnvironment cannot be instantiated when running in a pre-defined context " +
							"(such as Command Line Client, Scala Shell, or TestEnvironment)");
		}
		if (host == null) {
			throw new NullPointerException("Host must not be null.");
		}
		if (port < 1 || port >= 0xffff) {
			throw new IllegalArgumentException("Port out of range");
		}

		this.host = host;
		this.port = port;
		this.clientConfiguration = clientConfig == null ? new Configuration() : clientConfig;
		if (jarFiles != null) {
			this.jarFiles = new ArrayList<>(jarFiles.length);
			for (String jarFile : jarFiles) {
				try {
					this.jarFiles.add(new File(jarFile).getAbsoluteFile().toURI().toURL());
				} catch (MalformedURLException e) {
					throw new IllegalArgumentException("JAR file path invalid", e);
				}
			}
		}
		else {
			this.jarFiles = Collections.emptyList();
		}

		if (globalClasspaths == null) {
			this.globalClasspaths = Collections.emptyList();
		} else {
			this.globalClasspaths = Arrays.asList(globalClasspaths);
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		PlanExecutor executor = getExecutor();

		Plan p = createProgramPlan(jobName);

		// Session management is disabled, revert this commit to enable
		//p.setJobId(jobID);
		//p.setSessionTimeout(sessionTimeout);

		JobExecutionResult result = executor.executePlan(p);

		this.lastJobExecutionResult = result;
		return result;
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan p = createProgramPlan("plan", false);

		// make sure that we do not start an new executor here
		// if one runs, fine, of not, we create a local executor (lightweight) and let it
		// generate the plan
		if (executor != null) {
			return executor.getOptimizerPlanAsJSON(p);
		}
		else {
			PlanExecutor le = PlanExecutor.createLocalExecutor(null);
			String plan = le.getOptimizerPlanAsJSON(p);

			le.stop();

			return plan;
		}
	}

	@Override
	@PublicEvolving
	public void startNewSession() throws Exception {
		dispose();
		jobID = JobID.generate();
		installShutdownHook();
	}

	protected PlanExecutor getExecutor() throws Exception {
		if (executor == null) {
			executor = PlanExecutor.createRemoteExecutor(host, port, clientConfiguration,
				jarFiles, globalClasspaths);
			executor.setPrintStatusDuringExecution(getConfig().isSysoutLoggingEnabled());
		}

		// if we are using sessions, we keep the executor running
		if (getSessionTimeout() > 0 && !executor.isRunning()) {
			executor.start();
			installShutdownHook();
		}

		return executor;
	}

	// ------------------------------------------------------------------------
	//  Dispose
	// ------------------------------------------------------------------------

	protected void dispose() {
		// Remove shutdown hook to prevent resource leaks
		ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

		try {
			PlanExecutor executor = this.executor;
			if (executor != null) {
				executor.stop();
			}
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to dispose the session shutdown hook.");
		}
	}

	@Override
	public String toString() {
		return "Remote Environment (" + this.host + ":" + this.port + " - parallelism = " +
				(getParallelism() == -1 ? "default" : getParallelism()) + ") : " + getIdString();
	}

	// ------------------------------------------------------------------------
	//  Shutdown hooks and reapers
	// ------------------------------------------------------------------------

	private void installShutdownHook() {
		if (shutdownHook == null) {
			this.shutdownHook = ShutdownHookUtil.addShutdownHook(this::dispose, getClass().getSimpleName(), LOG);
		}
	}
}
