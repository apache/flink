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

package eu.stratosphere.nephele.streaming;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public final class PathLatency implements StreamingData {

	private final JobID jobID;

	private final ExecutionVertexID sourceID;

	private final ExecutionVertexID targetID;

	private double pathLatency;

	public PathLatency(final JobID jobID, final ExecutionVertexID sourceID, final ExecutionVertexID targetID,
			final double pathLatency) {

		if (jobID == null) {
			throw new IllegalArgumentException("jobID must not be null");
		}

		if (sourceID == null) {
			throw new IllegalArgumentException("sourceID must not be null");
		}

		if (targetID == null) {
			throw new IllegalArgumentException("targetID must not be null");
		}

		this.jobID = jobID;
		this.sourceID = sourceID;
		this.targetID = targetID;
		this.pathLatency = pathLatency;
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public PathLatency() {
		this.jobID = new JobID();
		this.sourceID = new ExecutionVertexID();
		this.targetID = new ExecutionVertexID();
		this.pathLatency = 0.0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.jobID.write(out);
		this.sourceID.write(out);
		this.targetID.write(out);
		out.writeDouble(this.pathLatency);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.jobID.read(in);
		this.sourceID.read(in);
		this.targetID.read(in);
		this.pathLatency = in.readDouble();
	}

	public JobID getJobID() {

		return this.jobID;
	}

	public ExecutionVertexID getSourceID() {

		return this.sourceID;
	}

	public ExecutionVertexID getTargetID() {

		return this.targetID;
	}

	public double getPathLatency() {

		return this.pathLatency;
	}
}
