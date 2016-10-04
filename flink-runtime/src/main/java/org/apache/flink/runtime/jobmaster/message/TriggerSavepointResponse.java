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

package org.apache.flink.runtime.jobmaster.message;

import org.apache.flink.api.common.JobID;

import java.io.Serializable;

/**
 * The response of the trigger savepoint request to JobManager.
 */
public abstract class TriggerSavepointResponse implements Serializable {

	private static final long serialVersionUID = 3139327824611807707L;

	private final JobID jobID;

	public JobID getJobID() {
		return jobID;
	}

	public TriggerSavepointResponse(final JobID jobID) {
		this.jobID = jobID;
	}

	public static class Success extends TriggerSavepointResponse implements Serializable {

		private static final long serialVersionUID = -1100637460388881776L;

		private final String savepointPath;

		public Success(final JobID jobID, final String savepointPath) {
			super(jobID);
			this.savepointPath = savepointPath;
		}

		public String getSavepointPath() {
			return savepointPath;
		}
	}

	public static class Failure extends TriggerSavepointResponse implements Serializable {

		private static final long serialVersionUID = -1668479003490615139L;

		private final Throwable cause;

		public Failure(final JobID jobID, final Throwable cause) {
			super(jobID);
			this.cause = cause;
		}

		public Throwable getCause() {
			return cause;
		}
	}
}

