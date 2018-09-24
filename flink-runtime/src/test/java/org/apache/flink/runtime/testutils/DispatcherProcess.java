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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.jobmanager.JobManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link Dispatcher} instance running in a separate JVM.
 */
public class DispatcherProcess extends TestJvmProcess {

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcess.class);

	/** Pattern to parse the job manager port from the logs. */
	private static final Pattern PORT_PATTERN = Pattern.compile(".*Actor system started at akka\\.tcp://flink@.*:(\\d+).*");

	/** ID for this JobManager. */
	private final int id;

	/** The configuration for the JobManager. */
	private final Configuration config;

	/** Configuration parsed as args for {@link JobManagerProcess.JobManagerProcessEntryPoint}. */
	private final String[] jvmArgs;

	/** The port the JobManager listens on. */
	private int jobManagerPort;

	/**
	 * Creates a {@link JobManager} running in a separate JVM.
	 *
	 * @param id     ID for the JobManager
	 * @param config Configuration for the job manager process
	 *
	 * @throws Exception
	 */
	public DispatcherProcess(int id, Configuration config) throws Exception {
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
		return DispatcherProcessEntryPoint.class.getName();
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

	@Override
	public String toString() {
		return String.format("JobManagerProcess(id=%d, port=%d)", id, jobManagerPort);
	}

	/**
	 * Entry point for the JobManager process.
	 */
	public static class DispatcherProcessEntryPoint {

		private static final Logger LOG = LoggerFactory.getLogger(DispatcherProcessEntryPoint.class);

		/**
		 * Entrypoint of the DispatcherProcessEntryPoint.
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

				config.setInteger(JobManagerOptions.PORT, 0);
				config.setInteger(RestOptions.PORT, 0);

				final StandaloneSessionClusterEntrypoint clusterEntrypoint = new StandaloneSessionClusterEntrypoint(config);

				ClusterEntrypoint.runClusterEntrypoint(clusterEntrypoint);
			}
			catch (Throwable t) {
				LOG.error("Failed to start JobManager process", t);
				System.exit(1);
			}
		}
	}
}
