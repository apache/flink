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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link TaskManager} instance running in a separate JVM.
 */
public class TaskManagerProcess extends TestJvmProcess {

	/** ID for this TaskManager */
	private final int id;

	/** The configuration for the TaskManager */
	private final Configuration config;

	/** Configuration parsed as args for {@link TaskManagerProcess.TaskManagerProcessEntryPoint} */
	private final String[] jvmArgs;

	public TaskManagerProcess(int id, Configuration config) throws Exception {
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
		return "TaskManager " + id;
	}

	@Override
	public String[] getJvmArgs() {
		return jvmArgs;
	}

	@Override
	public String getEntryPointClassName() {
		return TaskManagerProcessEntryPoint.class.getName();
	}

	public int getId() {
		return id;
	}

	@Override
	public String toString() {
		return String.format("TaskManagerProcess(id=%d)", id);
	}

	/**
	 * Entry point for the TaskManager process.
	 */
	public static class TaskManagerProcessEntryPoint {

		private static final Logger LOG = LoggerFactory.getLogger(TaskManagerProcessEntryPoint.class);

		/**
		 * All arguments are parsed to a {@link Configuration} and passed to the Taskmanager,
		 * for instance: <code>--recovery.mode ZOOKEEPER --recovery.zookeeper.quorum "xyz:123:456"</code>.
		 */
		public static void main(String[] args) throws Exception {
			try {
				Configuration config = ParameterTool.fromArgs(args).getConfiguration();

				if (!config.containsKey(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY)) {
					config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 4);
				}

				if (!config.containsKey(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY)) {
					config.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 100);
				}


				LOG.info("Configuration: {}.", config);

				// Run the TaskManager
				TaskManager.selectNetworkInterfaceAndRunTaskManager(config, TaskManager.class);

				// Run forever
				new CountDownLatch(1).await();
			}
			catch (Throwable t) {
				LOG.error("Failed to start TaskManager process", t);
				System.exit(1);
			}
		}
	}

}
