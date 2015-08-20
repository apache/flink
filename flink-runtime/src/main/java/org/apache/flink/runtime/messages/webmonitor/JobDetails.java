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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An actor message with a detailed overview of the current status of a job.
 */
public class JobDetails implements java.io.Serializable {

	private static final long serialVersionUID = -3391462110304948766L;
	
	private final JobID jobId;
	
	private final String jobName;
	
	private final long startTime;
	
	private final long endTime;
	
	private final JobStatus status;
	
	private final long lastUpdateTime;

	private final int[] numVerticesPerExecutionState;
	
	private final int numTasks;

	
	public JobDetails(JobID jobId, String jobName,
						long startTime, long endTime,
						JobStatus status,
						long lastUpdateTime,
						int[] numVerticesPerExecutionState, int numTasks) {
		
		this.jobId = checkNotNull(jobId);
		this.jobName = checkNotNull(jobName);
		this.startTime = startTime;
		this.endTime = endTime;
		this.status = checkNotNull(status);
		this.lastUpdateTime = lastUpdateTime;
		this.numVerticesPerExecutionState = checkNotNull(numVerticesPerExecutionState);
		this.numTasks = numTasks;
	}
	
	// ------------------------------------------------------------------------

	public JobID getJobId() {
		return jobId;
	}

	public String getJobName() {
		return jobName;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public JobStatus getStatus() {
		return status;
	}

	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	public int getNumTasks() {
		return numTasks;
	}

	public int[] getNumVerticesPerExecutionState() {
		return numVerticesPerExecutionState;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o != null && o.getClass() == JobDetails.class) {
			JobDetails that = (JobDetails) o;

			return this.endTime == that.endTime &&
					this.lastUpdateTime == that.lastUpdateTime &&
					this.numTasks == that.numTasks &&
					this.startTime == that.startTime &&
					this.status == that.status &&
					this.jobId.equals(that.jobId) &&
					this.jobName.equals(that.jobName) &&
					Arrays.equals(this.numVerticesPerExecutionState, that.numVerticesPerExecutionState);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = jobId.hashCode();
		result = 31 * result + jobName.hashCode();
		result = 31 * result + (int) (startTime ^ (startTime >>> 32));
		result = 31 * result + (int) (endTime ^ (endTime >>> 32));
		result = 31 * result + status.hashCode();
		result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
		result = 31 * result + Arrays.hashCode(numVerticesPerExecutionState);
		result = 31 * result + numTasks;
		return result;
	}

	@Override
	public String toString() {
		return "JobDetails {" +
				"jobId=" + jobId +
				", jobName='" + jobName + '\'' +
				", startTime=" + startTime +
				", endTime=" + endTime +
				", status=" + status +
				", lastUpdateTime=" + lastUpdateTime +
				", numVerticesPerExecutionState=" + Arrays.toString(numVerticesPerExecutionState) +
				", numTasks=" + numTasks +
				'}';
	}
}
