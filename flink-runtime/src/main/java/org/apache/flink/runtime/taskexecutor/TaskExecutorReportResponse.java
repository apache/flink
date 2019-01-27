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

package org.apache.flink.runtime.taskexecutor;

import java.io.Serializable;
import java.util.List;

/**
 * Base class for responses given to report from {@link TaskExecutionStatus}.
 */
public abstract class TaskExecutorReportResponse implements Serializable {

	private static final long serialVersionUID = 1L;

	// ----------------------------------------------------------------------------

	/**
	 * Base class for a successful report.
	 */
	public static class Success extends TaskExecutorReportResponse {
		private static final long serialVersionUID = 1L;

		/** The index of the rejected tasks in the report list. */
		private final List<Integer> rejectedIndexes;

		public Success(List<Integer> rejected) {
			this.rejectedIndexes = rejected;
		}

		/**
		 * Get the indexes of the rejected tasks.
		 */
		public List<Integer> getRejectedIndexes() {
			return rejectedIndexes;
		}

		@Override
		public String toString() {
			return "Report succeed with rejected(" + rejectedIndexes + ")";
		}
	}

	// ----------------------------------------------------------------------------

	/**
	 * A rejected (declined) registration.
	 */
	public static final class Decline extends TaskExecutorReportResponse {
		private static final long serialVersionUID = 1L;

		/** The reason of the rejection. */
		private final String reason;

		public Decline(String reason) {
			this.reason = reason != null ? reason : "(unknown)";
		}

		public String getReason() {
			return reason;
		}

		@Override
		public String toString() {
			return "Report declined (" + reason + ')';
		}
	}
}
