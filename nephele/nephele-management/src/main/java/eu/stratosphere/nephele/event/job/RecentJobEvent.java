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

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;

/**
 * A {@link RecentJobEvent} provides a summary of a job which is either currently running or has been running recently.
 * 
 * @author warneke
 */
public final class RecentJobEvent extends AbstractEvent implements ManagementEvent {

	/**
	 * The ID of the new job.
	 */
	private final JobID jobID;

	/**
	 * The name of the new job.
	 */
	private final String jobName;

	/**
	 * The last known status of the job.
	 */
	private final JobStatus jobStatus;

	/**
	 * <code>true</code> if profiling is enabled for this job, <code>false</code> otherwise.
	 */
	private final boolean isProfilingEnabled;

	/**
	 * The time stamp of the job submission.
	 */
	private final long submissionTimestamp;

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
		this.jobID = null;
		this.jobName = null;
		this.jobStatus = null;
		this.isProfilingEnabled = false;
		this.submissionTimestamp = -1L;
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

	/**
	 * {@inheritDoc}
	 */
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
