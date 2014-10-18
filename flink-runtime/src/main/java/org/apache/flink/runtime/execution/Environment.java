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


package org.apache.flink.runtime.execution;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.FutureTask;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.bufferprovider.BufferProvider;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.io.network.gates.GateID;
import org.apache.flink.runtime.io.network.gates.InputGate;
import org.apache.flink.runtime.io.network.gates.OutputGate;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.protocols.AccumulatorProtocol;

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
	 * @param gateID the gate ID
	 * @return the IDs of all the output channels connected to the gate with the given ID
	 */
	Set<ChannelID> getOutputChannelIDsOfGate(GateID gateID);

	/**
	 * Returns the IDs of all the input channels connected to the gate with the given ID.
	 *
	 * @param gateID the gate ID
	 * @return the IDs of all the input channels connected to the gate with the given ID
	 */
	Set<ChannelID> getInputChannelIDsOfGate(GateID gateID);

	/**
	 * Returns the proxy object for the accumulator protocol.
	 */
	AccumulatorProtocol getAccumulatorProtocolProxy();

	/**
	 * Returns the user code class loader
	 * @return user code class loader
	 */
	ClassLoader getUserClassLoader();

	/**
	 * Returns the buffer provider for this environment.
	 * <p/>
	 * The returned buffer provider is used by the output side of the network stack.
	 *
	 * @return Buffer provider for the output side of the network stack
	 * @see org.apache.flink.runtime.io.network.api.RecordWriter
	 */
	BufferProvider getOutputBufferProvider();

	Map<String, FutureTask<Path>> getCopyTask();
}
