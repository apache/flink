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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.configuration.Configuration;

/**
 * An {@link ExecutionEnvironment} that runs the program locally, multi-threaded, in the JVM where the
 * environment is instantiated.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 *
 * <p>Local environments can also be instantiated through {@link ExecutionEnvironment#createLocalEnvironment()}
 * and {@link ExecutionEnvironment#createLocalEnvironment(int)}. The former version will pick a
 * default parallelism equal to the number of hardware contexts in the local machine.
 */
@Public
public class LocalEnvironment extends ExecutionEnvironment {

	/** The user-defined configuration for the local execution. */
	private final Configuration configuration;

	/** Create lazily upon first use. */
	private PlanExecutor executor;

	/** In case we keep the executor alive for sessions, this reaper shuts it down eventually.
	 * The reaper's finalize method triggers the executor shutdown. */
	@SuppressWarnings("all")
	private ExecutorReaper executorReaper;

	/**
	 * Creates a new local environment.
	 */
	public LocalEnvironment() {
		this(new Configuration());
	}

	/**
	 * Creates a new local environment that configures its local executor with the given configuration.
	 *
	 * @param config The configuration used to configure the local executor.
	 */
	public LocalEnvironment(Configuration config) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The LocalEnvironment cannot be instantiated when running in a pre-defined context " +
							"(such as Command Line Client, Scala Shell, or TestEnvironment)");
		}
		this.configuration = config == null ? new Configuration() : config;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		if (executor == null) {
			startNewSession();
		}

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
		Plan p = createProgramPlan(null, false);

		// make sure that we do not start an executor in any case here.
		// if one runs, fine, of not, we only create the class but disregard immediately afterwards
		if (executor != null) {
			return executor.getOptimizerPlanAsJSON(p);
		}
		else {
			PlanExecutor tempExecutor = PlanExecutor.createLocalExecutor(configuration);
			return tempExecutor.getOptimizerPlanAsJSON(p);
		}
	}

	@Override
	@PublicEvolving
	public void startNewSession() throws Exception {
		if (executor != null) {
			// we need to end the previous session
			executor.stop();
			// create also a new JobID
			jobID = JobID.generate();
		}

		// create a new local executor
		executor = PlanExecutor.createLocalExecutor(configuration);
		executor.setPrintStatusDuringExecution(getConfig().isSysoutLoggingEnabled());

		// if we have a session, start the mini cluster eagerly to have it available across sessions
		if (getSessionTimeout() > 0) {
			executor.start();

			// also install the reaper that will shut it down eventually
			executorReaper = new ExecutorReaper(executor);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "Local Environment (parallelism = " + (getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? "default" : getParallelism())
				+ ") : " + getIdString();
	}

	// ------------------------------------------------------------------------
	//  Reaping the local executor when in session mode
	// ------------------------------------------------------------------------

	/**
	 * This thread shuts down the local executor.
	 *
	 * <p><b>IMPORTANT:</b> This must be a static inner class to hold no reference to the outer class.
	 * Otherwise, the outer class could never become garbage collectible while this thread runs.
	 */
	private static class ShutdownThread extends Thread {

		private final Object monitor = new Object();

		private final PlanExecutor executor;

		private volatile boolean triggered = false;

		ShutdownThread(PlanExecutor executor) {
			super("Local cluster reaper");
			setDaemon(true);
			setPriority(Thread.MIN_PRIORITY);

			this.executor = executor;
		}

		@Override
		public void run() {
			synchronized (monitor) {
				while (!triggered) {
					try {
						monitor.wait();
					}
					catch (InterruptedException e) {
						// should never happen
					}
				}
			}

			try {
				executor.stop();
			}
			catch (Throwable t) {
				System.err.println("Cluster reaper caught exception during shutdown");
				t.printStackTrace();
			}
		}

		void trigger() {
			triggered = true;
			synchronized (monitor) {
				monitor.notifyAll();
			}
		}

	}

	/**
	 * A class that, upon finalization, shuts down the local mini cluster by triggering the reaper
	 * thread.
	 */
	private static class ExecutorReaper {

		private final ShutdownThread shutdownThread;

		ExecutorReaper(PlanExecutor executor) {
			this.shutdownThread = new ShutdownThread(executor);
			this.shutdownThread.start();
		}

		@Override
		protected void finalize() throws Throwable {
			super.finalize();
			shutdownThread.trigger();
		}
	}
}
