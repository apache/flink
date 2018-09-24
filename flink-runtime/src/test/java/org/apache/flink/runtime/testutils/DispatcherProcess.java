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

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link Dispatcher} instance running in a separate JVM.
 */
public class DispatcherProcess extends TestJvmProcess {

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerProcess.class);

	/** ID for this JobManager. */
	private final int id;

	/** The configuration for the JobManager. */
	private final Configuration config;

	/** Configuration parsed as args for {@link JobManagerProcess.JobManagerProcessEntryPoint}. */
	private final String[] jvmArgs;

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

	@Override
	public String toString() {
		return String.format("JobManagerProcess(id=%d)", id);
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
