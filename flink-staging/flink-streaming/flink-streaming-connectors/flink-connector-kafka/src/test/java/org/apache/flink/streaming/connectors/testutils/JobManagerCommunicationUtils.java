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

package org.apache.flink.streaming.connectors.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class JobManagerCommunicationUtils {
	
	private static final FiniteDuration askTimeout = new FiniteDuration(30, TimeUnit.SECONDS);
	
	
	public static void cancelCurrentJob(ActorGateway jobManager) throws Exception {
		
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
			throw new Exception("Could not cancel job - no running jobs");
		}
		if (jobs.size() != 1) {
			throw new Exception("Could not cancel job - more than one running job.");
		}
		
		JobStatusMessage status = jobs.get(0);
		if (status.getJobState().isTerminalState()) {
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
