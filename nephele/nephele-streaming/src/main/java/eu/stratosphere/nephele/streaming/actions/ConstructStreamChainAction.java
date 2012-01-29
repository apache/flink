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

package eu.stratosphere.nephele.streaming.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.util.SerializableArrayList;

/**
 * This class implements an action to construct a stream chain for a particular sub-path of the graph.
 * 
 * @author warneke
 */
public final class ConstructStreamChainAction extends AbstractAction {

	private final SerializableArrayList<ExecutionVertexID> vertexIDs = new SerializableArrayList<ExecutionVertexID>();

	public ConstructStreamChainAction(final JobID jobID, final List<ExecutionVertexID> vertexIDs) {
		super(jobID);

		if (vertexIDs == null) {
			throw new IllegalArgumentException("Argument vertexIDs must not be null");
		}

		if (vertexIDs.size() < 2) {
			throw new IllegalArgumentException("Argument vertexIDs must be a list with at least two elements");
		}

		this.vertexIDs.addAll(vertexIDs);
	}

	public ConstructStreamChainAction() {
		super();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		super.write(out);

		this.vertexIDs.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		super.read(in);

		this.vertexIDs.read(in);
	}

	public List<ExecutionVertexID> getVertexIDs() {

		return Collections.unmodifiableList(this.vertexIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionVertexID getVertexID() {

		return this.vertexIDs.get(0);
	}
}
