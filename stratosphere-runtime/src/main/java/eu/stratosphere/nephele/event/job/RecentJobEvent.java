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

package eu.stratosphere.nephele.event.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * A {@link RecentJobEvent} provides a summary of a job which is either currently running or has been running recently.
 * 
 */
public final class RecentJobEvent extends AbstractEvent implements ManagementEvent {

	/**
	 * The ID of the new job.
	 */
	private JobID jobID;

	/**
	 * The name of the new job.
	 */
	private String jobName;

	/**
	 * The last known status of the job.
	 */
	private JobStatus jobStatus;

	/**
	 * <code>true</code> if profiling is enabled for this job, <code>false</code> otherwise.
	 */
	private boolean isProfilingEnabled;

	/**
	 * The time stamp of the job submission.
	 */
	private long submissionTimestamp;

	/**
	 * Constructs a new event.
	 * 
	 * @param jobID
	 *        the ID of the new job
	 * @param jobName
	 *        the name of the new job
	 * @param jobStatus
	 *        the status of the job
	 * @param isProfilingEnabled
	 *        <code>true</code> if profiling is enabled for this job, <code>false</code> otherwise
	 * @param submissionTimestamp
	 *        the time stamp of the job submission
	 * @param timestamp
	 *        the time stamp of the event
	 */
	public RecentJobEvent(final JobID jobID, final String jobName, final JobStatus jobStatus,
			final boolean isProfilingEnabled, final long submissionTimestamp, final long timestamp) {
		super(timestamp);

		if (jobStatus == null) {
			throw new IllegalArgumentException("job status must not be null");
		}

		this.jobID = jobID;
		this.jobName = jobName;
		this.jobStatus = jobStatus;
		this.isProfilingEnabled = isProfilingEnabled;
		this.submissionTimestamp = submissionTimestamp;
	}

	/**
	 * Constructor for serialization/deserialization. Should not be called on other occasions.
	 */
	public RecentJobEvent() {
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
	 * Returns the last known status of the job.
	 * 
	 * @return the last known status of the job
	 */
	public JobStatus getJobStatus() {
		return this.jobStatus;
	}

	/**
	 * Returns the time stamp of the job submission.
	 * 
	 * @return the time stamp of the job submission
	 */
	public long getSubmissionTimestamp() {

		return this.submissionTimestamp;
	}


	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);

		// Read the job ID
		this.jobID = new JobID();
		this.jobID.read(in);

		// Read the job name
		this.jobName = StringRecord.readString(in);

		// Read the job status
		this.jobStatus = EnumUtils.readEnum(in, JobStatus.class);

		// Read if profiling is enabled
		this.isProfilingEnabled = in.readBoolean();

		// Read the submission time stamp
		this.submissionTimestamp = in.readLong();
	}


	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);

		// Write the job ID
		this.jobID.write(out);

		// Write the job name
		StringRecord.writeString(out, this.jobName);

		// Writes the job status
		EnumUtils.writeEnum(out, this.jobStatus);

		// Write out if profiling is enabled
		out.writeBoolean(this.isProfilingEnabled);

		// Write out the submission time stamp
		out.writeLong(this.submissionTimestamp);
	}


	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof RecentJobEvent)) {
			return false;
		}

		final RecentJobEvent newJobEvent = (RecentJobEvent) obj;

		if (!this.jobID.equals(newJobEvent.getJobID())) {
			return false;
		}

		if (!this.jobName.equals(newJobEvent.getJobName())) {
			return false;
		}

		if (this.isProfilingEnabled != newJobEvent.isProfilingAvailable()) {
			return false;
		}

		if (this.submissionTimestamp != newJobEvent.getSubmissionTimestamp()) {
			return false;
		}

		return true;
	}


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
