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

package eu.stratosphere.nephele.checkpointing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.SerializableHashSet;

/**
 * A checkpoint replay request can be used to trigger the replay of a checkpoint at a remote task manager.
 * 
 * @author warneke
 */
public final class CheckpointReplayRequest implements IOReadableWritable {

	/**
	 * The vertex ID which identifies the checkpoint to be replayed.
	 */
	private ExecutionVertexID vertexID;

	/**
	 * The IDs of the output channels the replay task will be in charge of.
	 */
	private SerializableHashSet<ChannelID> outputChannelIDs = new SerializableHashSet<ChannelID>();

	/**
	 * Constructs a new checkpoint replay request.
	 * 
	 * @param vertexID
	 *        the vertex ID identifying the checkpoint to be replayed
	 */
	public CheckpointReplayRequest(final ExecutionVertexID vertexID) {

		if (vertexID == null) {
			throw new IllegalArgumentException("Argument vertexID must no be null");
		}

		this.vertexID = vertexID;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public CheckpointReplayRequest() {

		this.vertexID = new ExecutionVertexID();
	}

	/**
	 * Adds a set of channel IDs which identify the output channels the replay task will be in charge of.
	 * 
	 * @param outputChannelIDs
	 *        the IDs of the output channels the replay task will be in charge of
	 */
	public void addOutputChannelIDs(final Set<ChannelID> outputChannelIDs) {

		this.outputChannelIDs.addAll(outputChannelIDs);
	}

	/**
	 * Returns the set of channel IDs which identify the output channels the replay task will be in charge of.
	 * 
	 * @return the IDs of the output channels the replay task will be in charge of
	 */
	public Set<ChannelID> getOutputChannelIDs() {

		return Collections.unmodifiableSet(this.outputChannelIDs);
	}

	/**
	 * Returns the vertex ID identifying the checkpoint to be replayed.
	 * 
	 * @return the vertex ID identifying the checkpoint to be replayed
	 */
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.vertexID.write(out);

		this.outputChannelIDs.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.vertexID.read(in);

		this.outputChannelIDs.read(in);
	}
}
