/**
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


package org.apache.flink.runtime.profiling.types;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.managementgraph.ManagementVertexID;

/**
 * Input gate profiling events are a special subclass of vertex profiling events. They contain profiling information
 * which refer to particular input gates of a task.
 * <p>
 * This class is not thread-safe.
 * 
 */
public final class InputGateProfilingEvent extends VertexProfilingEvent {

	/**
	 * The index of the input gate at the corresponding management vertex.
	 */
	private int gateIndex;

	/**
	 * Stores how often the input gate had no records available during the last time period.
	 */
	private int noRecordsAvailableCounter;

	/**
	 * Constructs a new input gate profiling event.
	 * 
	 * @param gateIndex
	 *        the index of the input gate at the corresponding management vertex
	 * @param noRecordsAvailableCounter
	 *        indicates how often the input gate had no records available during the last time period
	 * @param vertexID
	 *        the ID of the management vertex this event refers to
	 * @param profilingInterval
	 *        the interval of time this profiling event covers
	 * @param jobID
	 *        the ID of the job this event refers to
	 * @param timestamp
	 *        the time stamp of the event
	 * @param profilingTimestamp
	 *        the time stamp of the profiling data
	 */
	public InputGateProfilingEvent(final int gateIndex, final int noRecordsAvailableCounter,
			final ManagementVertexID vertexID, final int profilingInterval, final JobID jobID, final long timestamp,
			final long profilingTimestamp) {

		super(vertexID, profilingInterval, jobID, timestamp, profilingTimestamp);

		this.gateIndex = gateIndex;
		this.noRecordsAvailableCounter = noRecordsAvailableCounter;
	}

	/**
	 * Default constructor for the serialization/deserialization process. Should not be called for other purposes.
	 */
	public InputGateProfilingEvent() {
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
	 * Returns the number of times no records were available
	 * on any of the channels attached to the input gate in
	 * the given profiling internval.
	 * 
	 * @return the number of times no records were available
	 */
	public int getNoRecordsAvailableCounter() {
		return this.noRecordsAvailableCounter;
	}


	@Override
	public void read(final DataInputView in) throws IOException {
		super.read(in);

		this.gateIndex = in.readInt();
		this.noRecordsAvailableCounter = in.readInt();
	}


	@Override
	public void write(final DataOutputView out) throws IOException {
		super.write(out);

		out.writeInt(this.gateIndex);
		out.writeInt(this.noRecordsAvailableCounter);
	}


	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof InputGateProfilingEvent)) {
			return false;
		}

		final InputGateProfilingEvent inputGateProfilingEvent = (InputGateProfilingEvent) obj;

		if (this.gateIndex != inputGateProfilingEvent.getGateIndex()) {
			return false;
		}

		if (this.noRecordsAvailableCounter != inputGateProfilingEvent.getNoRecordsAvailableCounter()) {
			return false;
		}

		return true;
	}


	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
