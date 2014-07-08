/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.execution;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.gates.GateID;
import eu.stratosphere.runtime.io.gates.InputGate;
import eu.stratosphere.runtime.io.gates.OutputGate;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.AccumulatorProtocol;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.template.InputSplitProvider;

/**
 * The user code of every Nephele task runs inside an <code>Environment</code> object. The environment provides
 * important services to the task. It keeps track of setting up the communication channels and provides access to input
 * splits, memory manager, etc.
 */
public interface Environment {
	/**
	 * Returns the ID of the job from the original job graph. It is used by the library cache manager to find the
	 * required
	 * libraries for executing the assigned Nephele task.
	 * 
	 * @return the ID of the job from the original job graph
	 */
	JobID getJobID();

	/**
	 * Returns the task configuration object which was attached to the original JobVertex.
	 * 
	 * @return the task configuration object which was attached to the original JobVertex.
	 */
	Configuration getTaskConfiguration();

	/**
	 * Returns the job configuration object which was attached to the original {@link JobGraph}.
	 * 
	 * @return the job configuration object which was attached to the original {@link JobGraph}
	 */
	Configuration getJobConfiguration();

	/**
	 * Returns the current number of subtasks the respective task is split into.
	 * 
	 * @return the current number of subtasks the respective task is split into
	 */
	int getCurrentNumberOfSubtasks();

	/**
	 * Returns the index of this subtask in the subtask group.
	 * 
	 * @return the index of this subtask in the subtask group
	 */
	int getIndexInSubtaskGroup();

	/**
	 * Sends a notification that objects that a new user thread has been started to the execution observer.
	 * 
	 * @param userThread
	 *        the user thread which has been started
	 */
	void userThreadStarted(Thread userThread);

	/**
	 * Sends a notification that a user thread has finished to the execution observer.
	 * 
	 * @param userThread
	 *        the user thread which has finished
	 */
	void userThreadFinished(Thread userThread);

	/**
	 * Returns the input split provider assigned to this environment.
	 * 
	 * @return the input split provider or <code>null</code> if no such provider has been assigned to this environment.
	 */
	InputSplitProvider getInputSplitProvider();

	/**
	 * Returns the current {@link IOManager}.
	 * 
	 * @return the current {@link IOManager}.
	 */
	IOManager getIOManager();

	/**
	 * Returns the current {@link MemoryManager}.
	 * 
	 * @return the current {@link MemoryManager}.
	 */
	MemoryManager getMemoryManager();

	/**
	 * Returns the name of the task running in this environment.
	 * 
	 * @return the name of the task running in this environment
	 */
	String getTaskName();

	/**
	 * Returns the next unbound input gate ID or <code>null</code> if no such ID exists
	 * 
	 * @return the next unbound input gate ID or <code>null</code> if no such ID exists
	 */
	GateID getNextUnboundInputGateID();

	/**
	 * Returns the number of output gates registered with this environment.
	 * 
	 * @return the number of output gates registered with this environment
	 */
	int getNumberOfOutputGates();

	/**
	 * Returns the number of input gates registered with this environment.
	 * 
	 * @return the number of input gates registered with this environment
	 */
	int getNumberOfInputGates();

	/**
	 * Returns the number of output channels attached to this environment.
	 * 
	 * @return the number of output channels attached to this environment
	 */
	int getNumberOfOutputChannels();

	/**
	 * Returns the number of input channels attached to this environment.
	 * 
	 * @return the number of input channels attached to this environment
	 */
	int getNumberOfInputChannels();

	/**
	 * Creates a new OutputGate and registers it with the Environment.
	 *
	 * @return the newly created output gate
	 */
	OutputGate createAndRegisterOutputGate();

	/**
	 * Creates a new InputGate and registers it with the Environment.
	 */
	<T extends IOReadableWritable> InputGate<T> createAndRegisterInputGate();

	/**
	 * Returns the IDs of all output channels connected to this environment.
	 * 
	 * @return the IDs of all output channels connected to this environment
	 */
	Set<ChannelID> getOutputChannelIDs();

	/**
	 * Returns the IDs of all input channels connected to this environment.
	 * 
	 * @return the IDs of all input channels connected to this environment
	 */
	Set<ChannelID> getInputChannelIDs();

	/**
	 * Returns the IDs of all output gates connected to this environment.
	 * 
	 * @return the IDs of all output gates connected to this environment
	 */
	Set<GateID> getOutputGateIDs();

	/**
	 * Returns the IDs of all input gates connected to this environment.
	 * 
	 * @return the IDs of all input gates connected to this environment
	 */
	Set<GateID> getInputGateIDs();

	/**
	 * Returns the IDs of all the output channels connected to the gate with the given ID.
	 * 
	 * @param gateID
	 *        the gate ID
	 * @return the IDs of all the output channels connected to the gate with the given ID
	 */
	Set<ChannelID> getOutputChannelIDsOfGate(GateID gateID);

	/**
	 * Returns the IDs of all the input channels connected to the gate with the given ID.
	 * 
	 * @param gateID
	 *        the gate ID
	 * @return the IDs of all the input channels connected to the gate with the given ID
	 */
	Set<ChannelID> getInputChannelIDsOfGate(GateID gateID);
	
	/**
	 * Returns the proxy object for the accumulator protocol.
	 */
	AccumulatorProtocol getAccumulatorProtocolProxy();

	/**
	 * Returns the buffer provider for this environment.
	 * <p>
	 * The returned buffer provider is used by the output side of the network stack.
	 *
	 * @return Buffer provider for the output side of the network stack
	 * @see eu.stratosphere.runtime.io.api.RecordWriter
	 */
	BufferProvider getOutputBufferProvider();
}
