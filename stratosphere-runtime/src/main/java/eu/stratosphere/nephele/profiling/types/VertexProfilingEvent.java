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

package eu.stratosphere.nephele.profiling.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * This interface is a base interface for profiling data which
 * pertains to the execution of tasks.
 * 
 * @author stanik
 */
public abstract class VertexProfilingEvent extends ProfilingEvent {

	private ManagementVertexID vertexID;

	private int profilingInterval;

	public VertexProfilingEvent(ManagementVertexID vertexID, int profilingInterval, JobID jobID, long timestamp,
			long profilingTimestamp) {
		super(jobID, timestamp, profilingTimestamp);

		this.vertexID = vertexID;
		this.profilingInterval = profilingInterval;
	}

	public VertexProfilingEvent() {
		super();
	}

	/**
	 * Returns the ID of the vertex this profiling information
	 * belongs to.
	 * 
	 * @return the ID of the vertex this profiling information belongs to
	 */
	public ManagementVertexID getVertexID() {
		return this.vertexID;
	}

	/**
	 * The interval in milliseconds to which the rest
	 * of the profiling data relates to.
	 * 
	 * @return the profiling interval given in milliseconds
	 */
	public int getProfilingInterval() {
		return this.profilingInterval;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		this.vertexID = new ManagementVertexID();
		this.vertexID.read(in);

		this.profilingInterval = in.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		this.vertexID.write(out);
		out.writeInt(this.profilingInterval);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof VertexProfilingEvent)) {
			return false;
		}

		final VertexProfilingEvent vertexProfilingEvent = (VertexProfilingEvent) obj;

		if (!this.vertexID.equals(vertexProfilingEvent.getVertexID())) {
			return false;
		}

		if (this.profilingInterval != vertexProfilingEvent.getProfilingInterval()) {
			return false;
		}

		return true;
	}
}
