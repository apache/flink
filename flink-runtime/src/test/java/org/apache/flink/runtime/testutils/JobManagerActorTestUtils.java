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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages.CurrentJobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages.JobStatusResponse;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.messages.JobManagerMessages.JobNotFound;
import static org.apache.flink.runtime.messages.JobManagerMessages.RequestJobStatus;
import static org.apache.flink.runtime.messages.JobManagerMessages.getRequestJobStatus;
import static org.apache.flink.runtime.messages.JobManagerMessages.getRequestNumberRegisteredTaskManager;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JobManager actor test utilities.
 *
 * <p>If you are using a {@link TestingJobManager} most of these are not needed.
 */
public class JobManagerActorTestUtils {

	/**
	 * Waits for the expected {@link JobStatus}.
	 *
	 * <p>Repeatedly queries the JobManager via {@link RequestJobStatus} messages.
	 *
	 * @param jobId             Job ID of the job to wait for
	 * @param expectedJobStatus Expected job status
	 * @param jobManager        Job manager actor to ask
	 * @param timeout           Timeout after which the operation fails
	 * @throws Exception If the job is not found within the timeout or the job is in another state.
	 */
	public static void waitForJobStatus(
			JobID jobId,
			JobStatus expectedJobStatus,
			ActorGateway jobManager,
			FiniteDuration timeout) throws Exception {

		checkNotNull(jobId, "Job ID");
		checkNotNull(expectedJobStatus, "Expected job status");
		checkNotNull(jobManager, "Job manager");
		checkNotNull(timeout, "Timeout");

		final Deadline deadline = timeout.fromNow();

		while (deadline.hasTimeLeft()) {
			// Request the job status
			JobStatusResponse response = requestJobStatus(jobId, jobManager, deadline.timeLeft());

			// Found the job
			if (response instanceof CurrentJobStatus) {
				JobStatus jobStatus = ((CurrentJobStatus) response).status();

				// OK, that's what we were waiting for
				if (jobStatus == expectedJobStatus) {
					return;
				}
				else if (jobStatus.isGloballyTerminalState()) {
					throw new IllegalStateException("Job is in terminal state " + jobStatus + ", "
							+ "but was waiting for " + expectedJobStatus + ".");
				}
			}
			// Did not find the job... retry
			else if (response instanceof JobNotFound) {
				Thread.sleep(Math.min(100, deadline.timeLeft().toMillis()));
			}
			else {
				throw new IllegalStateException("Unexpected response.");
			}
		}

		throw new IllegalStateException("Job not found within deadline.");
	}

	/**
	 * Request a {@link JobStatusResponse}.
	 *
	 * @param jobId      Job ID of the job to request the status of
	 * @param jobManager Job manager actor to ask
	 * @param timeout    Timeout after which the operation fails
	 * @return The {@link JobStatusResponse} from the job manager
	 * @throws Exception If there is no answer within the timeout.
	 */
	public static JobStatusResponse requestJobStatus(
			JobID jobId,
			ActorGateway jobManager,
			FiniteDuration timeout) throws Exception {

		checkNotNull(jobId, "Job ID");
		checkNotNull(jobManager, "Job manager");
		checkNotNull(timeout, "Timeout");

		// Ask the JobManager
		RequestJobStatus request = (RequestJobStatus) getRequestJobStatus(jobId);
		Future<Object> ask = jobManager.ask(request, timeout);
		Object response = Await.result(ask, timeout);

		if (response instanceof JobStatusResponse) {
			return (JobStatusResponse) response;
		}

		throw new IllegalStateException("Unexpected response.");
	}

	/**
	 * Waits for a minimum number of task managers to connect to the job manager.
	 *
	 * @param minimumNumberOfTaskManagers Minimum number of task managers to wait for
	 * @param jobManager                  Job manager actor to ask
	 * @param timeout                     Timeout after which the operation fails
	 * @throws Exception If the task managers don't connection with the timeout.
	 */
	public static void waitForTaskManagers(
			int minimumNumberOfTaskManagers,
			ActorGateway jobManager,
			FiniteDuration timeout) throws Exception {

		checkArgument(minimumNumberOfTaskManagers >= 1);
		checkNotNull(jobManager, "Job manager");
		checkNotNull(timeout, "Timeout");

		final Deadline deadline = timeout.fromNow();

		while (deadline.hasTimeLeft()) {
			Future<Object> ask = jobManager.ask(getRequestNumberRegisteredTaskManager(),
					deadline.timeLeft());

			Integer response = (Integer) Await.result(ask, deadline.timeLeft());

			// All are connected. We are done.
			if (response >= minimumNumberOfTaskManagers) {
				return;
			}
			// Waiting for more... retry
			else {
				Thread.sleep(Math.min(100, deadline.timeLeft().toMillis()));
			}
		}

		throw new IllegalStateException("Task managers not connected within deadline.");
	}
}
