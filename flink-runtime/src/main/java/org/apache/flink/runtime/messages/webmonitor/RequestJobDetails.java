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

/**
 * This message requests an overview of the jobs on the JobManager,
 * including running jobs and/or finished jobs.
 * <p>
 * The response to this message is a
 * {@link org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails} message.
 */
public class RequestJobDetails implements InfoMessage {

	private static final long serialVersionUID = 5208137000412166747L;
	
	private final boolean includeRunning;
	private final boolean includeFinished;

	public RequestJobDetails(boolean includeRunning, boolean includeFinished) {
		this.includeRunning = includeRunning;
		this.includeFinished = includeFinished;
	}
	
	// ------------------------------------------------------------------------

	public boolean shouldIncludeFinished() {
		return includeFinished;
	}

	public boolean shouldIncludeRunning() {
		return includeRunning;
	}

	// ------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o instanceof RequestJobDetails) {
			RequestJobDetails that = (RequestJobDetails) o;

			return this.includeFinished == that.includeFinished &&
					this.includeRunning == that.includeRunning;
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (includeRunning ? 31 : 0) + (includeFinished ? 1 : 0);
	}

	@Override
	public String toString() {
		return "RequestJobDetails{" +
				"includeRunning=" + includeRunning +
				", includeFinished=" + includeFinished +
				'}';
	}
}
