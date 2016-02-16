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

package org.apache.flink.runtime.testutils;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.JobManagerMode;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link JobManager} instance running in a separate JVM.
 */
public class JobManagerProcess extends TestJvmProcess {

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcess.class);

	/** ID for this JobManager */
	private final int id;

	/** The port the JobManager listens on */
	private final int jobManagerPort;

	/** The configuration for the JobManager */
	private final Configuration config;

	/** Configuration parsed as args for {@link JobManagerProcess.JobManagerProcessEntryPoint} */
	private final String[] jvmArgs;

	private ActorRef jobManagerRef;

	/**
	 * Creates a {@link JobManager} running in a separate JVM.
	 *
	 * <p>See {@link #JobManagerProcess(int, Configuration, int)} for a more
	 * detailed
	 * description.
	 *
	 * @param config Configuration for the job manager process
	 * @throws Exception
	 */
	public JobManagerProcess(int id, Configuration config) throws Exception {
		this(id, config, 0);
	}

	/**
	 * Creates a {@link JobManager} running in a separate JVM.
	 *
	 * @param id             ID for the JobManager
	 * @param config         Configuration for the job manager process
	 * @param jobManagerPort Job manager port (if <code>0</code>, pick any available port)
	 * @throws Exception
	 */
	public JobManagerProcess(int id, Configuration config, int jobManagerPort) throws Exception {
		checkArgument(id >= 0, "Negative ID");
		this.id = id;
		this.config = checkNotNull(config, "Configuration");
		this.jobManagerPort = jobManagerPort <= 0 ? NetUtils.getAvailablePort() : jobManagerPort;

		ArrayList<String> args = new ArrayList<>();
		args.add("--port");
		args.add(String.valueOf(this.jobManagerPort));

		for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
			args.add("--" + entry.getKey());
			args.add(entry.getValue());
		}

		this.jvmArgs = new String[args.size()];
		args.toArray(jvmArgs);
	}

	@Override
	public String getName() {
		return "JobManager " + id;
	}

	@Override
	public String[] getJvmArgs() {
		return jvmArgs;
	}

	@Override
	public String getEntryPointClassName() {
		return JobManagerProcessEntryPoint.class.getName();
	}

	public int getJobManagerPort() {
		return jobManagerPort;
	}

	public Configuration getConfig() {
		return config;
	}

	/**
	 * Returns the Akka URL of this JobManager.
	 */
	public String getJobManagerAkkaURL() {
		return JobManager.getRemoteJobManagerAkkaURL(
				new InetSocketAddress("localhost", jobManagerPort),
				Option.<String>empty());
	}

	@Override
	public String toString() {
		return String.format("JobManagerProcess(id=%d, port=%d)", id, jobManagerPort);
	}

	/**
	 * Waits for the job manager to be reachable.
	 *
	 * <p><strong>Important:</strong> Make sure to set the timeout larger than Akka's gating
	 * time. Otherwise, this will effectively not wait for the JobManager to startup, because the
	 * time out will fire immediately.
	 *
	 * @param actorSystem Actor system to be used to resolve JobManager address.
	 * @param timeout     Timeout (make sure to set larger than Akka's gating time).
	 */
	public ActorRef getActorRef(ActorSystem actorSystem, FiniteDuration timeout)
			throws Exception {

		if (jobManagerRef != null) {
			return jobManagerRef;
		}

		checkNotNull(actorSystem, "Actor system");

		// Deadline passes timeout ms
		Deadline deadline = timeout.fromNow();

		while (deadline.hasTimeLeft()) {
			try {
				// If the Actor is not reachable yet, this throws an Exception. Retry until the
				// deadline passes.
				this.jobManagerRef = AkkaUtils.getActorRef(
						getJobManagerAkkaURL(),
						actorSystem,
						deadline.timeLeft());

				return jobManagerRef;
			}
			catch (Throwable ignored) {
				// Retry
				Thread.sleep(Math.min(100, deadline.timeLeft().toMillis()));
			}
		}

		throw new IllegalStateException("JobManager did not start up within " + timeout + ".");
	}

	/**
	 * Entry point for the JobManager process.
	 */
	public static class JobManagerProcessEntryPoint {

		private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcessEntryPoint.class);

		/**
		 * Runs the JobManager process in {@link JobManagerMode#CLUSTER}.
		 *
		 * <p><strong>Required argument</strong>: <code>port</code>. Start the process with
		 * <code>--port PORT</code>.
		 *
		 * <p>Other arguments are parsed to a {@link Configuration} and passed to the
		 * JobManager, for instance: <code>--recovery.mode ZOOKEEPER --recovery.zookeeper.quorum
		 * "xyz:123:456"</code>.
		 */
		public static void main(String[] args) {
			try {
				ParameterTool params = ParameterTool.fromArgs(args);
				final int port = Integer.valueOf(params.getRequired("port"));
				LOG.info("Running on port {}.", port);

				Configuration config = params.getConfiguration();
				LOG.info("Configuration: {}.", config);

				// Run the JobManager
				JobManager.runJobManager(config, JobManagerMode.CLUSTER, "localhost", port);

				// Run forever. Forever, ever? Forever, ever!
				new CountDownLatch(1).await();
			}
			catch (Throwable t) {
				LOG.error("Failed to start JobManager process", t);
				System.exit(1);
			}
		}
	}

}
