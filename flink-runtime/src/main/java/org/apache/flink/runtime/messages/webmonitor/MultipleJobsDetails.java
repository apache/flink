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

import java.util.Arrays;

/**
 * An actor messages describing details of various jobs. This message is sent for example
 * in response to the {@link org.apache.flink.runtime.messages.webmonitor.RequestJobDetails}
 * message.
 */
public class MultipleJobsDetails implements java.io.Serializable {

	private static final long serialVersionUID = -1526236139616019127L;
	
	private static final JobDetails[] EMPTY = new JobDetails[0];
	
	private final JobDetails[] runningJobs;
	private final JobDetails[] finishedJobs;

	public MultipleJobsDetails(JobDetails[] running, JobDetails[] finished) {
		this.runningJobs = running == null ? EMPTY : running;
		this.finishedJobs = finished == null ? EMPTY : finished;
	}
	
	// ------------------------------------------------------------------------

	public JobDetails[] getRunningJobs() {
		return runningJobs;
	}

	public JobDetails[] getFinishedJobs() {
		return finishedJobs;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return Arrays.deepHashCode(runningJobs) + Arrays.deepHashCode(finishedJobs);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj instanceof MultipleJobsDetails) {
			MultipleJobsDetails that = (MultipleJobsDetails) obj;
			return Arrays.deepEquals(this.runningJobs, that.runningJobs) &&
					Arrays.deepEquals(this.finishedJobs, that.finishedJobs);
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "MultipleJobsDetails {" +
				"running=" + Arrays.toString(runningJobs) +
				", finished=" + Arrays.toString(finishedJobs) +
				'}';
	}
}
