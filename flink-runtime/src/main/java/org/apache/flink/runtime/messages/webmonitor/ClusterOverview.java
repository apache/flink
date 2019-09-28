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

import org.apache.flink.runtime.resourcemanager.ResourceOverview;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response to the {@link RequestStatusOverview} message, carrying a description
 * of the Flink cluster status.
 */
public class ClusterOverview extends JobsOverview {

	private static final long serialVersionUID = -729861859715105265L;

	public static final String FIELD_NAME_TASKMANAGERS = "taskmanagers";
	public static final String FIELD_NAME_SLOTS_TOTAL = "slots-total";
	public static final String FIELD_NAME_SLOTS_AVAILABLE = "slots-available";

	@JsonProperty(FIELD_NAME_TASKMANAGERS)
	private final int numTaskManagersConnected;

	@JsonProperty(FIELD_NAME_SLOTS_TOTAL)
	private final int numSlotsTotal;

	@JsonProperty(FIELD_NAME_SLOTS_AVAILABLE)
	private final int numSlotsAvailable;

	@JsonCreator
	public ClusterOverview(
			@JsonProperty(FIELD_NAME_TASKMANAGERS) int numTaskManagersConnected,
			@JsonProperty(FIELD_NAME_SLOTS_TOTAL) int numSlotsTotal,
			@JsonProperty(FIELD_NAME_SLOTS_AVAILABLE) int numSlotsAvailable,
			@JsonProperty(FIELD_NAME_JOBS_RUNNING) int numJobsRunningOrPending,
			@JsonProperty(FIELD_NAME_JOBS_FINISHED) int numJobsFinished,
			@JsonProperty(FIELD_NAME_JOBS_CANCELLED) int numJobsCancelled,
			@JsonProperty(FIELD_NAME_JOBS_FAILED) int numJobsFailed) {

		super(numJobsRunningOrPending, numJobsFinished, numJobsCancelled, numJobsFailed);
		
		this.numTaskManagersConnected = numTaskManagersConnected;
		this.numSlotsTotal = numSlotsTotal;
		this.numSlotsAvailable = numSlotsAvailable;
	}

	public ClusterOverview(int numTaskManagersConnected, int numSlotsTotal, int numSlotsAvailable,
						   JobsOverview jobs1, JobsOverview jobs2) {
		super(jobs1, jobs2);
		this.numTaskManagersConnected = numTaskManagersConnected;
		this.numSlotsTotal = numSlotsTotal;
		this.numSlotsAvailable = numSlotsAvailable;
	}

	public ClusterOverview(ResourceOverview resourceOverview, JobsOverview jobsOverview) {
		this(
			resourceOverview.getNumberTaskManagers(),
			resourceOverview.getNumberRegisteredSlots(),
			resourceOverview.getNumberFreeSlots(),
			jobsOverview.getNumJobsRunningOrPending(),
			jobsOverview.getNumJobsFinished(),
			jobsOverview.getNumJobsCancelled(),
			jobsOverview.getNumJobsFailed());
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
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj instanceof ClusterOverview) {
			ClusterOverview that = (ClusterOverview) obj;
			return this.numTaskManagersConnected == that.numTaskManagersConnected &&
					this.numSlotsTotal == that.numSlotsTotal &&
					this.numSlotsAvailable == that.numSlotsAvailable &&
					this.getNumJobsRunningOrPending() == that.getNumJobsRunningOrPending() &&
					this.getNumJobsFinished() == that.getNumJobsFinished() &&
					this.getNumJobsCancelled() == that.getNumJobsCancelled() &&
					this.getNumJobsFailed() == that.getNumJobsFailed();
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + numTaskManagersConnected;
		result = 31 * result + numSlotsTotal;
		result = 31 * result + numSlotsAvailable;
		return result;
	}

	@Override
	public String toString() {
		return "StatusOverview {" +
				"numTaskManagersConnected=" + numTaskManagersConnected +
				", numSlotsTotal=" + numSlotsTotal +
				", numSlotsAvailable=" + numSlotsAvailable +
				", numJobsRunningOrPending=" + getNumJobsRunningOrPending() +
				", numJobsFinished=" + getNumJobsFinished() +
				", numJobsCancelled=" + getNumJobsCancelled() +
				", numJobsFailed=" + getNumJobsFailed() +
				'}';
	}
}
