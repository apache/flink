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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;

import java.util.List;
import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Utilities for communicating with a jobmanager through a {@link ActorGateway}.
 */
public class JobManagerCommunicationUtils {

	private static final FiniteDuration askTimeout = new FiniteDuration(30, TimeUnit.SECONDS);

	public static void waitUntilNoJobIsRunning(ActorGateway jobManager) throws Exception {
		while (true) {
			// find the jobID
			Future<Object> listResponse = jobManager.ask(
					JobManagerMessages.getRequestRunningJobsStatus(), askTimeout);

			Object result = Await.result(listResponse, askTimeout);
			List<JobStatusMessage> jobs = ((JobManagerMessages.RunningJobsStatus) result).getStatusMessages();

			if (jobs.isEmpty()) {
				return;
			}

			Thread.sleep(50);
		}
	}

	public static void waitUntilJobIsRunning(ActorGateway jobManager, String name) throws Exception {
		while (true) {
			Future<Object> listResponse = jobManager.ask(
				JobManagerMessages.getRequestRunningJobsStatus(),
				askTimeout);

			List<JobStatusMessage> jobs;
			try {
				Object result = Await.result(listResponse, askTimeout);
				jobs = ((JobManagerMessages.RunningJobsStatus) result).getStatusMessages();
			}
			catch (Exception e) {
				throw new Exception("Could not wait for job to start - failed to retrieve running jobs from the JobManager.", e);
			}

			// see if the running jobs contain the requested job
			for (JobStatusMessage job : jobs) {
				if (job.getJobName().equals(name)) {
					return;
				}
			}

			Thread.sleep(50);
		}
	}

	public static void cancelCurrentJob(ActorGateway jobManager) throws Exception {
		cancelCurrentJob(jobManager, null);
	}

	public static void cancelCurrentJob(ActorGateway jobManager, String name) throws Exception {
		JobStatusMessage status = null;

		for (int i = 0; i < 200; i++) {
			// find the jobID
			Future<Object> listResponse = jobManager.ask(
					JobManagerMessages.getRequestRunningJobsStatus(),
					askTimeout);

			List<JobStatusMessage> jobs;
			try {
				Object result = Await.result(listResponse, askTimeout);
				jobs = ((JobManagerMessages.RunningJobsStatus) result).getStatusMessages();
			}
			catch (Exception e) {
				throw new Exception("Could not cancel job - failed to retrieve running jobs from the JobManager.", e);
			}

			if (jobs.isEmpty()) {
				// try again, fall through the loop
				Thread.sleep(50);
			}
			else if (jobs.size() == 1) {
				status = jobs.get(0);
			}
			else if (name != null) {
				for (JobStatusMessage msg: jobs) {
					if (msg.getJobName().equals(name)) {
						status = msg;
					}
				}
				if (status == null) {
					throw new Exception("Could not cancel job - no job matched expected name = '" + name + "' in " + jobs);
				}
			} else {
				String jobNames = "";
				for (JobStatusMessage jsm: jobs) {
					jobNames += jsm.getJobName() + ", ";
				}
				throw new Exception("Could not cancel job - more than one running job: " + jobNames);
			}
		}

		if (status == null) {
			throw new Exception("Could not cancel job - no running jobs");
		}
		else if (status.getJobState().isGloballyTerminalState()) {
			throw new Exception("Could not cancel job - job is not running any more");
		}

		JobID jobId = status.getJobId();

		Future<Object> response = jobManager.ask(new JobManagerMessages.CancelJob(jobId), askTimeout);
		try {
			Await.result(response, askTimeout);
		}
		catch (Exception e) {
			throw new Exception("Sending the 'cancel' message failed.", e);
		}
	}
}
