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

package eu.stratosphere.nephele.execution;

import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;

/**
 * The user code of every Nephele task runs inside an <code>Environment</code> object. The environment provides
 * important services to the task. It keeps track of setting up the communication channels and provides access to input
 * splits, memory manager, etc.
 * 
 * @author warneke
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
	 * Returns the task configuration object which was attached to the original {@link JobVertex}.
	 * 
	 * @return the task configuration object which was attached to the original {@link JobVertex}
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
	
	// DW: Start of temporary code
	void reportPACTDataStatistics(final long numberOfConsumedBytes, final long numberOfProducedBytes);
	// DW: End of temporary code

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
	 * Returns the next unbound output gate ID or <code>null</code> if no such ID exists
	 * 
	 * @return the next unbound output gate ID or <code>null</code> if no such ID exists
	 */
	GateID getNextUnboundOutputGateID();

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
	 * Creates an output gate.
	 * 
	 * @param gateID
	 * @param outputClass
	 * @param selector
	 * @param isBroadcast
	 * @return the created output gate
	 */
	OutputGate<? extends Record> createOutputGate(GateID gateID, Class<? extends Record> outputClass,
			ChannelSelector<? extends Record> selector, boolean isBroadcast);

	/**
	 * Creates an input gate.
	 * 
	 * @param gateID
	 * @param deserializer
	 * @param distributionPattern
	 * @return the created input gate
	 */
	InputGate<? extends Record> createInputGate(GateID gateID, RecordDeserializer<? extends Record> deserializer,
			DistributionPattern distributionPattern);

	/**
	 * Registers an output gate with this environment.
	 * 
	 * @param outputGate
	 *        the output gate to be registered
	 */
	void registerOutputGate(OutputGate<? extends Record> outputGate);

	/**
	 * Registers an input gate with this environment.
	 * 
	 * @param inputGate
	 *        the input gate to be registered
	 */
	void registerInputGate(InputGate<? extends Record> inputGate);

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
}
