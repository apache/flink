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
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.util.FlinkException;

/**
 * This exception is the base exception for all exceptions that denote any failure during
 * the execution of a job, and signal the failure of an application with a given
 * {@link ApplicationStatus}.
 */
public class JobExecutionException extends FlinkException {

	private static final long serialVersionUID = 2818087325120827525L;

	private final JobID jobID;

	private final ApplicationStatus status;

	/**
	 * Constructs a new job execution exception.
	 *
	 * @param jobID  The job's ID.
	 * @param status The application status.
	 * @param msg    The cause for the execution exception.
	 * @param cause  The cause of the exception
	 */
	public JobExecutionException(JobID jobID, ApplicationStatus status, String msg, Throwable cause) {
		super(msg, cause);
		this.jobID = jobID;
		this.status = status;
	}

	/**
	 * Constructs a new job execution exception.
	 *
	 * @param jobID  The job's ID.
	 * @param status The application status.
	 * @param msg    The cause for the execution exception.
	 */
	public JobExecutionException(JobID jobID, ApplicationStatus status, String msg) {
		super(msg);
		this.jobID = jobID;
		this.status = status;
	}

	/**
	 * Constructs a new job execution exception.
	 *
	 * @param jobID  The job's ID.
	 * @param status The application status.
	 * @param cause  The cause of the exception
	 */
	public JobExecutionException(JobID jobID, ApplicationStatus status, Throwable cause) {
		super(cause);
		this.jobID = jobID;
		this.status = status;
	}

	public JobID getJobID() {
		return jobID;
	}

	public ApplicationStatus getStatus() {
		return status;
	}
}
