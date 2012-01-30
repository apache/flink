/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.InputGateListener;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.OutputGateListener;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.types.Record;

/**
 * This interface must be implemented by profiling components
 * for the task manager manager.
 * 
 * @author warneke
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
	 * Registers a {@link InputGateListener} object for the given input gate.
	 * 
	 * @param id
	 *        the ID of the vertex the given input gate belongs to
	 * @param jobConfiguration
	 *        the configuration of the job the vertex belongs to
	 * @param inputGate
	 *        the input gate to register a {@link InputGateListener} object for
	 */
	void registerInputGateListener(ExecutionVertexID id, Configuration jobConfiguration,
			InputGate<? extends Record> inputGate);

	/**
	 * Registers a {@link OutputGateListener} object for the given output gate.
	 * 
	 * @param id
	 *        the ID of the vertex the given output gate belongs to
	 * @param jobConfiguration
	 *        the configuration of the job the vertex belongs to
	 * @param outputGate
	 *        the output gate to register a {@link InputGateListener} object for
	 */
	void registerOutputGateListener(ExecutionVertexID id, Configuration jobConfiguration,
			OutputGate<? extends Record> outputGate);

	/**
	 * Unregisters all previously register {@link ExecutionListener} objects for
	 * the vertex identified by the given ID.
	 * 
	 * @param id
	 *        the ID of the vertex to unregister the {@link ExecutionListener} objects for
	 */
	void unregisterExecutionListener(ExecutionVertexID id);

	/**
	 * Unregisters all previously register {@link InputGateListener} objects for
	 * the vertex identified by the given ID.
	 * 
	 * @param id
	 *        the ID of the vertex to unregister the {@link InputGateListener} objects for
	 */
	void unregisterInputGateListeners(ExecutionVertexID id);

	/**
	 * Unregisters all previously register {@link OutputGateListener} objects for
	 * the vertex identified by the given ID.
	 * 
	 * @param id
	 *        the ID of the vertex to unregister the {@link OutputGateListener} objects for
	 */
	void unregisterOutputGateListeners(ExecutionVertexID id);

	/**
	 * Shuts done the task manager's profiling component
	 * and stops all its internal processes.
	 */
	void shutdown();
}
