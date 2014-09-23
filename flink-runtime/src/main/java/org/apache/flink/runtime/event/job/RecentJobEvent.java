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

package org.apache.flink.runtime.event.job;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * A {@link RecentJobEvent} provides a summary of a job which is either currently running or has been running recently.
 */
public final class RecentJobEvent extends AbstractEvent implements ManagementEvent {

	private static final long serialVersionUID = -3361778351490181333L;

	/** The ID of the new job. */
	private JobID jobID;

	/** The name of the new job. */
	private String jobName;

	/** The last known status of the job. */
	private JobStatus jobStatus;

	/** <code>true</code> if profiling is enabled for this job, <code>false</code> otherwise. */
	private boolean isProfilingEnabled;

	/** The time stamp of the job submission. */
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
	public RecentJobEvent(JobID jobID, String jobName, JobStatus jobStatus,
			boolean isProfilingEnabled, long submissionTimestamp, long timestamp) {
		super(timestamp);

		Preconditions.checkNotNull(jobID);
		Preconditions.checkNotNull(jobStatus);

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
		this.jobID = new JobID();
	}

	// --------------------------------------------------------------------------------------------
	
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

	// --------------------------------------------------------------------------------------------
	//  Serialization
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(final DataInputView in) throws IOException {
		super.read(in);
		
		this.jobID.read(in);
		this.jobName = StringUtils.readNullableString(in);
		this.jobStatus = JobStatus.values()[in.readInt()];
		this.isProfilingEnabled = in.readBoolean();
		this.submissionTimestamp = in.readLong();
	}

	@Override
	public void write(final DataOutputView out) throws IOException {
		super.write(out);

		this.jobID.write(out);
		StringUtils.writeNullableString(jobName, out);
		out.writeInt(jobStatus.ordinal());
		out.writeBoolean(this.isProfilingEnabled);
		out.writeLong(this.submissionTimestamp);
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RecentJobEvent) {
			final RecentJobEvent other = (RecentJobEvent) obj;
			return super.equals(other) && this.jobID.equals(other.jobID) && this.isProfilingEnabled == other.isProfilingEnabled &&
					this.submissionTimestamp == other.submissionTimestamp &&
					(this.jobName == null ? other.jobName == null : (other.jobName != null &&
						this.jobName.equals(other.jobName)));
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ jobID.hashCode() ^ jobStatus.ordinal();
	}
	
	@Override
	public String toString() {
		return String.format("RecentJobEvent #%d at %s - jobId=%s, jobName=%s, status=%s, jobSubmission=%s, profiling=%s",
				getSequenceNumber(), getTimestampString(), jobID, jobName, jobStatus, timestampToString(submissionTimestamp),
				isProfilingEnabled);
	}
}
