/*
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
import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.api.common.JobID;

import com.google.common.base.Preconditions;

/**
 * A profiling event is a special type of event. It is intended to transport profiling data of a Nephele job to external
 * components.
 */
public abstract class ProfilingEvent extends AbstractEvent {

	private static final long serialVersionUID = 1L;

	/** The ID of the job the profiling data belongs to. */
	private final JobID jobID;

	/** The profiling time stamp. */
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
	public ProfilingEvent(JobID jobID, long timestamp, long profilingTimestamp) {
		super(timestamp);

		Preconditions.checkNotNull(jobID);
		this.jobID = jobID;
		this.profilingTimestamp = profilingTimestamp;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public ProfilingEvent() {
		super();
		this.jobID = new JobID();
	}

	// --------------------------------------------------------------------------------------------
	
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

	// --------------------------------------------------------------------------------------------
	//  Serialization
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		this.jobID.read(in);
		this.profilingTimestamp = in.readLong();
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		this.jobID.write(out);
		out.writeLong(this.profilingTimestamp);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ProfilingEvent) {
			final ProfilingEvent other = (ProfilingEvent) obj;
			
			return super.equals(obj) && this.profilingTimestamp == other.profilingTimestamp &&
					this.jobID.equals(other.jobID);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return this.jobID.hashCode() ^ ((int) (profilingTimestamp >>> 32)) ^ ((int) (profilingTimestamp)) ^
				super.hashCode();
	}
}
