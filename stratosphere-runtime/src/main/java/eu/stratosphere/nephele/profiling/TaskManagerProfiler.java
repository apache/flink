/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * This interface must be implemented by profiling components
 * for the task manager manager.
 * 
 */
public interface TaskManagerProfiler {

	/**
	 * Registers an {@link ExecutionListener} object for profiling.
	 * 
	 * @param task
	 *        task to be register a profiling listener for
	 * @param jobConfiguration
	 *        the job configuration sent with the task
	 */
	void registerExecutionListener(RuntimeTask task, Configuration jobConfiguration);


	/**
	 * Unregisters all previously register {@link ExecutionListener} objects for
	 * the vertex identified by the given ID.
	 * 
	 * @param id
	 *        the ID of the vertex to unregister the {@link ExecutionListener} objects for
	 */
	void unregisterExecutionListener(ExecutionVertexID id);

	/**
	 * Shuts done the task manager's profiling component
	 * and stops all its internal processes.
	 */
	void shutdown();
}
