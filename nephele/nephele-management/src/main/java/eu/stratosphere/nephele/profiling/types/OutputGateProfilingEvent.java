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
 * Through this interface it is possible to access profiling data about
 * the utilization of input gates.
 * 
 * @author stanik
 */
public class OutputGateProfilingEvent extends VertexProfilingEvent {

	private int gateIndex;

	private int channelCapacityExhausted;

	public OutputGateProfilingEvent(int gateIndex, int channelCapacityExhausted, ManagementVertexID vertexID,
			int profilingInterval, JobID jobID, long timestamp, long profilingTimestamp) {
		super(vertexID, profilingInterval, jobID, timestamp, profilingTimestamp);

		this.gateIndex = gateIndex;
		this.channelCapacityExhausted = channelCapacityExhausted;
	}

	public OutputGateProfilingEvent() {
		super();
	}

	/**
	 * Returns the index of input gate.
	 * 
	 * @return the index of the input gate
	 */
	public int getGateIndex() {
		return this.gateIndex;
	}

	/**
	 * Returns the number of times the capacity of an attached
	 * output channel was exhausted during the given profiling
	 * interval.
	 * 
	 * @return the number of times a channel reached its capacity limit
	 */
	public int getChannelCapacityExhausted() {
		return this.channelCapacityExhausted;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		this.gateIndex = in.readInt();
		this.channelCapacityExhausted = in.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(this.gateIndex);
		out.writeInt(this.channelCapacityExhausted);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof OutputGateProfilingEvent)) {
			return false;
		}

		final OutputGateProfilingEvent outputGateProfilingEvent = (OutputGateProfilingEvent) obj;

		if (this.gateIndex != outputGateProfilingEvent.getGateIndex()) {
			return false;
		}

		if (this.channelCapacityExhausted != outputGateProfilingEvent.getChannelCapacityExhausted()) {
			return false;
		}

		return true;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		
		return super.hashCode();
	}
}
