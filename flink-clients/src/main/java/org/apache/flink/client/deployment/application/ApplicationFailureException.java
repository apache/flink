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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.FlinkException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Exception that signals the failure of an
 * application with a given {@link ApplicationStatus}.
 */
@Internal
public class ApplicationFailureException extends FlinkException {

	private final JobID jobID;

	private final ApplicationStatus status;

	public ApplicationFailureException(
			final JobID jobID,
			final ApplicationStatus status,
			final String message,
			final Throwable cause) {
		super(message, cause);
		this.jobID = jobID;
		this.status = checkNotNull(status);
	}

	public JobID getJobID() {
		return jobID;
	}

	public ApplicationStatus getStatus() {
		return status;
	}

	public static ApplicationFailureException fromJobResult(
			final JobResult result,
			final ClassLoader userClassLoader) {

		checkState(result != null && !result.isSuccess());
		checkNotNull(userClassLoader);

		final JobID jobID = result.getJobId();
		final ApplicationStatus status = result.getApplicationStatus();

		final Throwable throwable = result
				.getSerializedThrowable()
				.map(ser -> ser.deserializeError(userClassLoader))
				.orElse(new FlinkException("Unknown reason."));

		if (status == ApplicationStatus.CANCELED || status == ApplicationStatus.FAILED) {
			return new ApplicationFailureException(
					jobID, status, "Application Status: " + status.name(), throwable);
		}

		return new ApplicationFailureException(
				jobID, ApplicationStatus.UNKNOWN, "Job failed for unknown reason.", throwable);
	}
}
