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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;

import java.util.HashMap;
import java.util.Map;

/**
 * This class describes the basic parameters for launching a TaskManager process.
 */
public class ContaineredTaskManagerParameters implements java.io.Serializable {

	private static final long serialVersionUID = -3096987654278064670L;

	/** The number of slots per TaskManager. */
	private final int numSlots;

	/** Environment variables to add to the Java process. */
	private final HashMap<String, String> taskManagerEnv;

	private final TaskExecutorResourceSpec taskExecutorResourceSpec;

	public ContaineredTaskManagerParameters(
			TaskExecutorResourceSpec taskExecutorResourceSpec,
			int numSlots,
			HashMap<String, String> taskManagerEnv) {

		this.taskExecutorResourceSpec = taskExecutorResourceSpec;
		this.numSlots = numSlots;
		this.taskManagerEnv = taskManagerEnv;
	}

	// ------------------------------------------------------------------------

	public TaskExecutorResourceSpec getTaskExecutorResourceSpec() {
		return taskExecutorResourceSpec;
	}

	public int numSlots() {
		return numSlots;
	}

	public Map<String, String> taskManagerEnv() {
		return taskManagerEnv;
	}


	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "TaskManagerParameters {" +
			"taskExecutorResourceSpec=" + taskExecutorResourceSpec +
			", numSlots=" + numSlots +
			", taskManagerEnv=" + taskManagerEnv +
			'}';
	}

	// ------------------------------------------------------------------------
	//  Factory
	// ------------------------------------------------------------------------

	/**
	 * Computes the parameters to be used to start a TaskManager Java process.
	 *
	 * @param config The Flink configuration.
	 * @param taskExecutorResourceSpec The resource specifics of the task executor.
	 * @param numSlots Number of slots of the task executor.
	 * @return The parameters to start the TaskManager processes with.
	 */
	public static ContaineredTaskManagerParameters create(
			Configuration config,
			TaskExecutorResourceSpec taskExecutorResourceSpec,
			int numSlots) {

		// obtain the additional environment variables from the configuration
		final HashMap<String, String> envVars = new HashMap<>();
		final String prefix = ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX;

		for (String key : config.keySet()) {
			if (key.startsWith(prefix) && key.length() > prefix.length()) {
				// remove prefix
				String envVarKey = key.substring(prefix.length());
				envVars.put(envVarKey, config.getString(key, null));
			}
		}

		// done
		return new ContaineredTaskManagerParameters(
			taskExecutorResourceSpec, numSlots, envVars);
	}
}
