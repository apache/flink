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

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * Output gate profiling events are a special subclass of vertex profiling events. They contain profiling information
 * which refer to particular output gates of a task.
 * <p>
 * This class is not thread-safe.
 * 
 * @author stanik
 */
public final class OutputGateProfilingEvent extends VertexProfilingEvent {

	/**
	 * The index of the output gate at the corresponding management vertex.
	 */
	private final int gateIndex;

	/**
	 * Stores how often the output gate had exhausted one of its channels capacity during the last time period.
	 */
	private final int channelCapacityExhausted;

	/**
	 * Constructs a new output gate profiling event.
	 * 
	 * @param gateIndex
	 *        the index of the output gate at the corresponding management vertex
	 * @param channelCapacityExhausted
	 *        indicates how often the output gate had exhausted one of its channels capacity during the last time period
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
	public OutputGateProfilingEvent(final int gateIndex, final int channelCapacityExhausted,
			final ManagementVertexID vertexID, final int profilingInterval, final JobID jobID, final long timestamp,
			final long profilingTimestamp) {
		super(vertexID, profilingInterval, jobID, timestamp, profilingTimestamp);

		this.gateIndex = gateIndex;
		this.channelCapacityExhausted = channelCapacityExhausted;
	}

	/**
	 * Default constructor for the serialization/deserialization process. Should not be called for other purposes.
	 */
	public OutputGateProfilingEvent() {
		this.gateIndex = -1;
		this.channelCapacityExhausted = -1;
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
	 * Returns the number of times the capacity of an attached output channel was exhausted during the given profiling
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
	public boolean equals(final Object obj) {

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
