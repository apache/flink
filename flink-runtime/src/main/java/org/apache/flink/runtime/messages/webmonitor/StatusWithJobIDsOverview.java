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

import java.util.List;

/**
 * Response to the {@link org.apache.flink.runtime.messages.webmonitor.RequestStatusWithJobIDsOverview}
 * message, carrying a description of the Flink cluster status.
 */
public class StatusWithJobIDsOverview extends JobsWithIDsOverview {

	private static final long serialVersionUID = -729861859715105265L;
	
	private final int numTaskManagersConnected;
	private final int numSlotsTotal;
	private final int numSlotsAvailable;

	public StatusWithJobIDsOverview(int numTaskManagersConnected, int numSlotsTotal, int numSlotsAvailable,
									List<JobID> jobsRunningOrPending, List<JobID> jobsFinished,
									List<JobID> jobsCancelled, List<JobID> jobsFailed) {

		super(jobsRunningOrPending, jobsFinished, jobsCancelled, jobsFailed);
		
		this.numTaskManagersConnected = numTaskManagersConnected;
		this.numSlotsTotal = numSlotsTotal;
		this.numSlotsAvailable = numSlotsAvailable;
	}

	public StatusWithJobIDsOverview(int numTaskManagersConnected, int numSlotsTotal, int numSlotsAvailable,
									JobsWithIDsOverview jobs1, JobsWithIDsOverview jobs2) {
		super(jobs1, jobs2);
		this.numTaskManagersConnected = numTaskManagersConnected;
		this.numSlotsTotal = numSlotsTotal;
		this.numSlotsAvailable = numSlotsAvailable;
	}

	public int getNumTaskManagersConnected() {
		return numTaskManagersConnected;
	}

	public int getNumSlotsTotal() {
		return numSlotsTotal;
	}

	public int getNumSlotsAvailable() {
		return numSlotsAvailable;
	}
	
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "StatusOverview {" +
				"numTaskManagersConnected=" + numTaskManagersConnected +
				", numSlotsTotal=" + numSlotsTotal +
				", numSlotsAvailable=" + numSlotsAvailable +
				", numJobsRunningOrPending=" + getJobsRunningOrPending() +
				", numJobsFinished=" + getJobsFinished() +
				", numJobsCancelled=" + getJobsCancelled() +
				", numJobsFailed=" + getJobsFailed() +
				'}';
	}
}
