/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.ManagementEvent;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * A profiling event is a special type of event. It is intended to transport profiling data of a Nephele job to external
 * components.
 * <p>
 * This class is not thread-safe.
 * 
 */
public abstract class ProfilingEvent extends AbstractEvent implements ManagementEvent {

	/**
	 * The ID of the job the profiling data belongs to.
	 */
	private JobID jobID;

	/**
	 * The profiling time stamp.
	 */
	private long profilingTimestamp;

	/**
	 * Constructs a new profiling event.
	 * 
	 * @param jobID
	 *        the ID of the job this profiling events belongs to
	 * @param timestamp
	 *        the time stamp of the event
	 * @param profilingTimestamp
	 *        the time stamp of the profiling data
	 */
	public ProfilingEvent(final JobID jobID, final long timestamp, final long profilingTimestamp) {
		super(timestamp);

		this.jobID = jobID;
		this.profilingTimestamp = profilingTimestamp;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public ProfilingEvent() {
		super();
	}

	/**
	 * Returns the ID of the job this profiling information belongs to.
	 * 
	 * @return the ID of the job this profiling information belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the timestamp of this profiling information. The timestamp denotes
	 * the time period in milliseconds between the creation of this profiling information
	 * and the start of the corresponding vertex's execution.
	 * 
	 * @return the timestamp of this profiling information.
	 */
	public long getProfilingTimestamp() {
		return this.profilingTimestamp;
	}


	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);

		this.jobID = new JobID();
		this.jobID.read(in);

		this.profilingTimestamp = in.readLong();
	}


	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);

		this.jobID.write(out);
		out.writeLong(this.profilingTimestamp);
	}


	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof ProfilingEvent)) {
			return false;
		}

		final ProfilingEvent profilingEvent = (ProfilingEvent) obj;

		if (!this.jobID.equals(profilingEvent.getJobID())) {
			return false;
		}

		if (this.profilingTimestamp != profilingEvent.getProfilingTimestamp()) {
			return false;
		}

		return true;
	}


	@Override
	public int hashCode() {

		if (this.jobID != null) {
			return this.jobID.hashCode();
		}

		return super.hashCode();
	}
}
