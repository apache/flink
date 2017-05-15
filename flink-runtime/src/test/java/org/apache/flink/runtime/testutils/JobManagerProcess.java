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
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.JobManagerMode;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link JobManager} instance running in a separate JVM.
 */
public class JobManagerProcess extends TestJvmProcess {

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcess.class);

	/** Pattern to parse the job manager port from the logs. */
	private static final Pattern PORT_PATTERN = Pattern.compile(".*Starting JobManager at akka\\.tcp://flink@.*:(\\d+).*");

	/** ID for this JobManager */
	private final int id;

	/** The configuration for the JobManager */
	private final Configuration config;

	/** Configuration parsed as args for {@link JobManagerProcess.JobManagerProcessEntryPoint} */
	private final String[] jvmArgs;

	/** The port the JobManager listens on */
	private int jobManagerPort;

	private ActorRef jobManagerRef;

	/**
	 * Creates a {@link JobManager} running in a separate JVM.
	 *
	 * @param id     ID for the JobManager
	 * @param config Configuration for the job manager process
	 *
	 * @throws Exception
	 */
	public JobManagerProcess(int id, Configuration config) throws Exception {
		checkArgument(id >= 0, "Negative ID");
		this.id = id;
		this.config = checkNotNull(config, "Configuration");

		ArrayList<String> args = new ArrayList<>();

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

	public Configuration getConfig() {
		return config;
	}

	/**
	 * Parses the port from the job manager logs and returns it.
	 *
	 * <p>If a call to this method succeeds, successive calls will directly
	 * return the port and re-parse the logs.
	 *
	 * @param timeout Timeout for log parsing.
	 * @return The port of the job manager
	 * @throws InterruptedException  If interrupted while waiting before
	 *                               retrying to parse the logs
	 * @throws NumberFormatException If the parsed port is not a number
	 */
	public int getJobManagerPort(FiniteDuration timeout) throws InterruptedException, NumberFormatException {
		if (jobManagerPort > 0) {
			return jobManagerPort;
		} else {
			Deadline deadline = timeout.fromNow();
			while (deadline.hasTimeLeft()) {
				Matcher matcher = PORT_PATTERN.matcher(getProcessOutput());
				if (matcher.find()) {
					String port = matcher.group(1);
					jobManagerPort = Integer.parseInt(port);
					return jobManagerPort;
				} else {
					Thread.sleep(100);
				}
			}

			throw new RuntimeException("Could not parse port from logs");
		}
	}

	/**
	 * Returns the Akka URL of this JobManager.
	 */
	public String getJobManagerAkkaURL(FiniteDuration timeout) throws InterruptedException, UnknownHostException {
		int port = getJobManagerPort(timeout);

		return AkkaRpcServiceUtils.getRpcUrl(
			"localhost",
			port,
			JobMaster.JOB_MANAGER_NAME,
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION,
			config);
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
						getJobManagerAkkaURL(deadline.timeLeft()),
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
		 * JobManager, for instance: <code>--high-availability ZOOKEEPER --high-availability.zookeeper.quorum
		 * "xyz:123:456"</code>.
		 */
		public static void main(String[] args) {
			try {
				ParameterTool params = ParameterTool.fromArgs(args);
				Configuration config = params.getConfiguration();
				LOG.info("Configuration: {}.", config);

				// Run the JobManager
				JobManager.runJobManager(config, JobManagerMode.CLUSTER, "localhost", 0);

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
