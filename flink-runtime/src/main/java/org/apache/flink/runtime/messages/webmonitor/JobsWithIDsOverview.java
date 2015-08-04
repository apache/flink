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

import java.util.ArrayList;
import java.util.List;

/**
 * An overview of how many jobs are in which status.
 */
public class JobsWithIDsOverview implements InfoMessage {

	private static final long serialVersionUID = -3699051943490133183L;
	
	private final List<JobID> jobsRunningOrPending;
	private final List<JobID> jobsFinished;
	private final List<JobID> jobsCancelled;
	private final List<JobID> jobsFailed;

	public JobsWithIDsOverview(List<JobID> jobsRunningOrPending, List<JobID> jobsFinished, 
								List<JobID> jobsCancelled, List<JobID> jobsFailed) {
		
		this.jobsRunningOrPending = jobsRunningOrPending;
		this.jobsFinished = jobsFinished;
		this.jobsCancelled = jobsCancelled;
		this.jobsFailed = jobsFailed;
	}

	public JobsWithIDsOverview(JobsWithIDsOverview first, JobsWithIDsOverview second) {
		this.jobsRunningOrPending = combine(first.getJobsRunningOrPending(), second.getJobsRunningOrPending());
		this.jobsFinished = combine(first.getJobsFinished(), second.getJobsFinished());
		this.jobsCancelled = combine(first.getJobsCancelled(), second.getJobsCancelled());
		this.jobsFailed = combine(first.getJobsFailed(), second.getJobsFailed());
	}

	public List<JobID> getJobsRunningOrPending() {
		return jobsRunningOrPending;
	}

	public List<JobID> getJobsFinished() {
		return jobsFinished;
	}

	public List<JobID> getJobsCancelled() {
		return jobsCancelled;
	}

	public List<JobID> getJobsFailed() {
		return jobsFailed;
	}
	
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "JobsOverview {" +
				"numJobsRunningOrPending=" + jobsRunningOrPending +
				", numJobsFinished=" + jobsFinished +
				", numJobsCancelled=" + jobsCancelled +
				", numJobsFailed=" + jobsFailed +
				'}';
	}

	// ------------------------------------------------------------------------

	private static ArrayList<JobID> combine(List<JobID> first, List<JobID> second) {
		ArrayList<JobID> result = new ArrayList<JobID>(first.size() + second.size());
		result.addAll(first);
		result.addAll(second);
		return result;
	}
}
