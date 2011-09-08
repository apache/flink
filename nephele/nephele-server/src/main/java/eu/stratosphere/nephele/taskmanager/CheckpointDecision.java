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

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;

public final class CheckpointDecision implements IOReadableWritable {

	/**
	 * The ID of the vertex the checkpoint decision applies to.
	 */
	private final ExecutionVertexID vertexID;

	/**
	 * The checkpoint decision itself.
	 */
	private boolean checkpointRequired = false;

	/**
	 * Constructs a new checkpoint decision object.
	 * 
	 * @param vertexID
	 *        the ID of the vertex the checkpoint decision applies to
	 * @param checkpointRequired
	 *        <code>true</code> to indicate the checkpoint shall be materialized, <code>false</code> to discard it
	 */
	public CheckpointDecision(final ExecutionVertexID vertexID, final boolean checkpointRequired) {
		this.vertexID = vertexID;
		this.checkpointRequired = checkpointRequired;
	}

	/**
	 * Default constructor required for serialized/deserialization.
	 */
	public CheckpointDecision() {
		this.vertexID = new ExecutionVertexID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.vertexID.write(out);
		out.writeBoolean(this.checkpointRequired);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.vertexID.read(in);
		this.checkpointRequired = in.readBoolean();
	}

	/**
	 * Returns the ID of the vertex the checkpoint decision applies to.
	 * 
	 * @return the ID of the vertex the checkpoint decision applies to
	 */
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	/**
	 * Returns the checkpoint decision itself.
	 * 
	 * @return <code>true</code> to indicate that the checkpoint shall be materialized, <code>false</code> to discard it
	 */
	public boolean getCheckpointDecision() {

		return this.checkpointRequired;
	}
}
