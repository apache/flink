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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;

import java.io.IOException;

/**
 * A simple registry that tracks if a certain job is pending execution, running, or completed.
 *
 * <p>This registry is used in highly-available setups with multiple master nodes,
 * to determine whether a new leader should attempt to recover a certain job (because the
 * job is still running), or whether the job has already finished successfully (in case of a
 * finite job) and the leader has only been granted leadership because the previous leader
 * quit cleanly after the job was finished.
 *
 * <p>In addition, the registry can help to determine whether a newly assigned leader JobManager
 * should attempt reconciliation with running TaskManagers, or immediately schedule the job from
 * the latest checkpoint/savepoint.
 */
public interface RunningJobsRegistry {

	/**
	 * The scheduling status of a job, as maintained by the {@code RunningJobsRegistry}.
	 */
	enum JobSchedulingStatus {

		/** Job has not been scheduled, yet. */
		PENDING,

		/** Job has been scheduled and is not yet finished. */
		RUNNING,

		/** Job has been finished, successfully or unsuccessfully. */
		DONE;
	}

	// ------------------------------------------------------------------------

	/**
	 * Marks a job as running. Requesting the job's status via the {@link #getJobSchedulingStatus(JobID)}
	 * method will return {@link JobSchedulingStatus#RUNNING}.
	 *
	 * @param jobID The id of the job.
	 *
	 * @throws IOException Thrown when the communication with the highly-available storage or registry
	 *                     failed and could not be retried.
	 */
	void setJobRunning(JobID jobID) throws IOException;

	/**
	 * Marks a job as completed. Requesting the job's status via the {@link #getJobSchedulingStatus(JobID)}
	 * method will return {@link JobSchedulingStatus#DONE}.
	 *
	 * @param jobID The id of the job.
	 *
	 * @throws IOException Thrown when the communication with the highly-available storage or registry
	 *                     failed and could not be retried.
	 */
	void setJobFinished(JobID jobID) throws IOException;

	/**
	 * Gets the scheduling status of a job.
	 *
	 * @param jobID The id of the job to check.
	 * @return The job scheduling status.
	 *
	 * @throws IOException Thrown when the communication with the highly-available storage or registry
	 *                     failed and could not be retried.
	 */
	JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException;

	/**
	 * Clear job state form the registry, usually called after job finish.
	 *
	 * @param jobID The id of the job to check.
	 *
	 * @throws IOException Thrown when the communication with the highly-available storage or registry
	 *                     failed and could not be retried.
	 */
	void clearJob(JobID jobID) throws IOException;
}
