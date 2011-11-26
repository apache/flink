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

package eu.stratosphere.nephele.taskmanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.SerializableHashSet;

/**
 * A task submission wrapper is simply a wrapper class which bundles a task's execution vertex ID, its execution
 * environment and its configuration object for an RPC call.
 * 
 * @author warneke
 */
public final class TaskSubmissionWrapper implements IOReadableWritable {

	/**
	 * The task's execution vertex ID.
	 */
	ExecutionVertexID vertexID = null;

	/**
	 * The task's execution environment.
	 */
	RuntimeEnvironment environment = null;

	/**
	 * The task's configuration object.
	 */
	Configuration configuration = null;

	/**
	 * The set of initially active output channels.
	 */
	SerializableHashSet<ChannelID> activeOutputChannels;

	/**
	 * Constructs a new task submission wrapper.
	 * 
	 * @param vertexID
	 *        the task's execution vertex ID
	 * @param environment
	 *        the task's execution environment
	 * @param configuration
	 *        the task's configuration
	 */
	public TaskSubmissionWrapper(final ExecutionVertexID vertexID, final RuntimeEnvironment environment,
			final Configuration configuration, final SerializableHashSet<ChannelID> activeOutputChannels) {

		if (vertexID == null) {
			throw new IllegalArgumentException("Argument vertexID is null");
		}

		if (environment == null) {
			throw new IllegalArgumentException("Argument environment is null");
		}

		if (configuration == null) {
			throw new IllegalArgumentException("Argument configuration is null");
		}

		if (activeOutputChannels == null) {
			throw new IllegalArgumentException("Argument activeOutputChannels is null");
		}

		this.vertexID = vertexID;
		this.environment = environment;
		this.configuration = configuration;
		this.activeOutputChannels = activeOutputChannels;
	}

	/**
	 * The default constructor for serialization/deserialization.
	 */
	public TaskSubmissionWrapper() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.vertexID.write(out);
		this.environment.write(out);
		this.configuration.write(out);
		this.activeOutputChannels.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.vertexID = new ExecutionVertexID();
		this.vertexID.read(in);
		this.environment = new RuntimeEnvironment();
		this.environment.read(in);
		this.configuration = new Configuration();
		this.configuration.read(in);
		this.activeOutputChannels = new SerializableHashSet<ChannelID>();
		this.activeOutputChannels.read(in);
	}

	/**
	 * Returns the task's execution vertex ID.
	 * 
	 * @return the task's execution vertex ID
	 */
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	/**
	 * Returns the task's execution environment.
	 * 
	 * @return the task's execution environment
	 */
	public RuntimeEnvironment getEnvironment() {

		return this.environment;
	}

	/**
	 * Returns the task's configuration object.
	 * 
	 * @return the task's configuration object
	 */
	public Configuration getConfiguration() {

		return this.configuration;
	}

	/**
	 * Returns the set of initially active output channels.
	 * 
	 * @return the set of initially active output channels
	 */
	public Set<ChannelID> getActiveOutputChannels() {

		return this.activeOutputChannels;
	}
}
