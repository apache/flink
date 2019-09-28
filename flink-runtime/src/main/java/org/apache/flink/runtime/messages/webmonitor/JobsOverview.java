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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

/**
 * An overview of how many jobs are in which status.
 */
public class JobsOverview implements InfoMessage {

	private static final long serialVersionUID = -3699051943490133183L;

	public static final String FIELD_NAME_JOBS_RUNNING = "jobs-running";
	public static final String FIELD_NAME_JOBS_FINISHED = "jobs-finished";
	public static final String FIELD_NAME_JOBS_CANCELLED = "jobs-cancelled";
	public static final String FIELD_NAME_JOBS_FAILED = "jobs-failed";

	@JsonProperty(FIELD_NAME_JOBS_RUNNING)
	private final int numJobsRunningOrPending;

	@JsonProperty(FIELD_NAME_JOBS_FINISHED)
	private final int numJobsFinished;

	@JsonProperty(FIELD_NAME_JOBS_CANCELLED)
	private final int numJobsCancelled;

	@JsonProperty(FIELD_NAME_JOBS_FAILED)
	private final int numJobsFailed;

	@JsonCreator
	public JobsOverview(
			@JsonProperty(FIELD_NAME_JOBS_RUNNING) int numJobsRunningOrPending,
			@JsonProperty(FIELD_NAME_JOBS_FINISHED) int numJobsFinished,
			@JsonProperty(FIELD_NAME_JOBS_CANCELLED) int numJobsCancelled,
			@JsonProperty(FIELD_NAME_JOBS_FAILED) int numJobsFailed) {

		this.numJobsRunningOrPending = numJobsRunningOrPending;
		this.numJobsFinished = numJobsFinished;
		this.numJobsCancelled = numJobsCancelled;
		this.numJobsFailed = numJobsFailed;
	}

	public JobsOverview(JobsOverview first, JobsOverview second) {
		this.numJobsRunningOrPending = first.numJobsRunningOrPending + second.numJobsRunningOrPending;
		this.numJobsFinished = first.numJobsFinished + second.numJobsFinished;
		this.numJobsCancelled = first.numJobsCancelled + second.numJobsCancelled;
		this.numJobsFailed = first.numJobsFailed + second.numJobsFailed;
	}

	public int getNumJobsRunningOrPending() {
		return numJobsRunningOrPending;
	}

	public int getNumJobsFinished() {
		return numJobsFinished;
	}

	public int getNumJobsCancelled() {
		return numJobsCancelled;
	}

	public int getNumJobsFailed() {
		return numJobsFailed;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj instanceof JobsOverview) {
			JobsOverview that = (JobsOverview) obj;
			return this.numJobsRunningOrPending == that.numJobsRunningOrPending &&
					this.numJobsFinished == that.numJobsFinished &&
					this.numJobsCancelled == that.numJobsCancelled &&
					this.numJobsFailed == that.numJobsFailed;
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = numJobsRunningOrPending;
		result = 31 * result + numJobsFinished;
		result = 31 * result + numJobsCancelled;
		result = 31 * result + numJobsFailed;
		return result;
	}

	@Override
	public String toString() {
		return "JobsOverview {" +
				"numJobsRunningOrPending=" + numJobsRunningOrPending +
				", numJobsFinished=" + numJobsFinished +
				", numJobsCancelled=" + numJobsCancelled +
				", numJobsFailed=" + numJobsFailed +
				'}';
	}

	/**
	 * Combines the given jobs overview with this.
	 *
	 * @param jobsOverview to combine with this
	 * @return Combined jobs overview
	 */
	public JobsOverview combine(JobsOverview jobsOverview) {
		return new JobsOverview(this, jobsOverview);
	}

	public static JobsOverview create(Collection<JobStatus> allJobsStatus) {
		Preconditions.checkNotNull(allJobsStatus);

		int numberRunningOrPendingJobs = 0;
		int numberFinishedJobs = 0;
		int numberCancelledJobs = 0;
		int numberFailedJobs = 0;

		for (JobStatus status : allJobsStatus) {
			switch (status) {
				case FINISHED:
					numberFinishedJobs++;
					break;
				case FAILED:
					numberFailedJobs++;
					break;
				case CANCELED:
					numberCancelledJobs++;
					break;
				default:
					numberRunningOrPendingJobs++;
					break;
			}
		}

		return new JobsOverview(numberRunningOrPendingJobs, numberFinishedJobs, numberCancelledJobs, numberFailedJobs);
	}
}
