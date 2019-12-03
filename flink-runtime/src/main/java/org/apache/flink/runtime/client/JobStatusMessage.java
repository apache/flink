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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

/**
 * A simple message that holds the state of a job execution.
 */
public class JobStatusMessage implements java.io.Serializable {

	private final JobID jobId;

	private final String jobName;

	private final JobStatus jobState;

	private final long startTime;

	public JobStatusMessage(JobID jobId, String jobName, JobStatus jobState, long startTime) {
		this.jobId = jobId;
		this.jobName = jobName;
		this.jobState = jobState;
		this.startTime = startTime;
	}

	public JobID getJobId() {
		return jobId;
	}

	public String getJobName() {
		return jobName;
	}

	public JobStatus getJobState() {
		return jobState;
	}

	public long getStartTime() {
		return startTime;
	}
}
