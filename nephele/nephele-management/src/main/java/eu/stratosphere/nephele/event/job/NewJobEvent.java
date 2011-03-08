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

package eu.stratosphere.nephele.event.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * A {@link NewJobEvent} can be used to notify other objects about the arrival of a new Nephele job.
 * 
 * @author warneke
 */
public final class NewJobEvent extends AbstractEvent implements ManagementEvent {

	/**
	 * The ID of the new job.
	 */
	private JobID jobID;

	/**
	 * The name of the new job.
	 */
	private String jobName;

	/**
	 * <code>true</code> if profiling is enabled for this job, <code>false</code> otherwise.
	 */
	private boolean isProfilingEnabled;

	/**
	 * Constructs a new event.
	 * 
	 * @param jobID
	 *        the ID of the new job
	 * @param jobName
	 *        the name of the new job
	 * @param isProfilingEnabled
	 *        <code>true</code> if profiling is enabled for this job, <code>false</code> otherwise
	 * @param timestamp
	 *        the time stamp of the event
	 */
	public NewJobEvent(final JobID jobID, final String jobName, final boolean isProfilingEnabled, final long timestamp) {
		super(timestamp);

		this.jobID = jobID;
		this.jobName = jobName;
		this.isProfilingEnabled = isProfilingEnabled;
	}

	/**
	 * Constructor for serialization/deserialization. Should not be called on other occasions.
	 */
	public NewJobEvent() {
		super();
	}

	/**
	 * Returns the ID of the new job.
	 * 
	 * @return the ID of the new job
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the name of the new job.
	 * 
	 * @return the name of the new job or <code>null</code> if the job has no name
	 */
	public String getJobName() {
		return this.jobName;
	}

	/**
	 * Checks if profiling is enabled for the new job.
	 * 
	 * @return <code>true</code> if profiling is enabled for this job, <code>false</code> otherwise
	 */
	public boolean isProfilingAvailable() {
		return this.isProfilingEnabled;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);

		// Read the job ID
		this.jobID = new JobID();
		this.jobID.read(in);

		// Read the job name
		this.jobName = StringRecord.readString(in);

		// Read if profiling is enabled
		this.isProfilingEnabled = in.readBoolean();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);

		// Write the job ID
		this.jobID.write(out);

		// Write the job name
		StringRecord.writeString(out, this.jobName);

		// Write out if profiling is enabled
		out.writeBoolean(this.isProfilingEnabled);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof NewJobEvent)) {
			return false;
		}

		final NewJobEvent newJobEvent = (NewJobEvent) obj;

		if (!this.jobID.equals(newJobEvent.getJobID())) {
			return false;
		}

		if (!this.jobName.equals(newJobEvent.getJobName())) {
			return false;
		}

		if (this.isProfilingEnabled != newJobEvent.isProfilingAvailable()) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		if (this.jobID != null) {
			return this.jobID.hashCode();
		}

		if (this.jobName != null) {
			return this.jobName.hashCode();
		}

		return super.hashCode();
	}
}
