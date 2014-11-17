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

package org.apache.flink.runtime.profiling;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.Task;

/**
 * This interface must be implemented by profiling components
 * for the task manager manager.
 */
public interface TaskManagerProfiler {

	/**
	 * Registers a {@link org.apache.flink.runtime.taskmanager.Task} object for profiling.
	 * 
	 * @param task
	 *        task to be register a profiling listener for
	 * @param jobConfiguration
	 *        the job configuration sent with the task
	 */
	void registerTask(Task task, Configuration jobConfiguration);

	/**
	 * Unregisters all previously registered {@link org.apache.flink.runtime.taskmanager.Task}
	 * objects for the vertex identified by the given ID.
	 * 
	 * @param id
	 *        the ID of the vertex to unregister the
	 *        {@link org.apache.flink.runtime.taskmanager.Task} objects for
	 */
	void unregisterTask(ExecutionAttemptID id);

	/**
	 * Shuts done the task manager's profiling component
	 * and stops all its internal processes.
	 */
	void shutdown();
}
