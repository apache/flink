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

package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class stores information about the latency of a specific (sub) path from a start to an end vertex.
 * 
 * @author warneke
 */
public final class TaskLatency extends AbstractStreamingData {

	/**
	 * The ID of the vertex this task latency information refers to
	 */
	private final ExecutionVertexID vertexID;

	/**
	 * The task latency in milliseconds
	 */
	private double taskLatency;

	/**
	 * Constructs a new task latency object.
	 * 
	 * @param jobID
	 *        the ID of the job this path latency information refers to
	 * @param vertexID
	 *        the ID of the vertex this task latency information refers to
	 * @param taskLatency
	 *        the task latency in milliseconds
	 */
	public TaskLatency(final JobID jobID, final ExecutionVertexID vertexID, final double taskLatency) {

		super(jobID);

		if (vertexID == null) {
			throw new IllegalArgumentException("vertexID must not be null");
		}

		this.vertexID = vertexID;
		this.taskLatency = taskLatency;
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public TaskLatency() {
		super(new JobID());
		this.vertexID = new ExecutionVertexID();
		this.taskLatency = 0.0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		this.vertexID.write(out);
		out.writeDouble(this.taskLatency);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.vertexID.read(in);
		this.taskLatency = in.readDouble();
	}

	/**
	 * Returns the ID of the vertex this task latency information refers to.
	 * 
	 * @return the ID of the vertex this task latency information refers to
	 */
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	/**
	 * Returns the task latency in milliseconds.
	 * 
	 * @return the task latency in milliseconds
	 */
	public double getTaskLatency() {

		return this.taskLatency;
	}
}
