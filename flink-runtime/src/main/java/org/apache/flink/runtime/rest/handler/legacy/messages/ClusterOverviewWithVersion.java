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

package org.apache.flink.runtime.rest.handler.legacy.messages;

import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Cluster overview message including the current Flink version and commit id.
 */
public class ClusterOverviewWithVersion extends ClusterOverview implements ResponseBody {

	private static final long serialVersionUID = 5000058311783413216L;

	public static final String FIELD_NAME_VERSION = "flink-version";
	public static final String FIELD_NAME_COMMIT = "flink-commit";

	@JsonProperty(FIELD_NAME_VERSION)
	private final String version;

	@JsonProperty(FIELD_NAME_COMMIT)
	private final String commitId;

	@JsonCreator
	public ClusterOverviewWithVersion(
			@JsonProperty(FIELD_NAME_TASKMANAGERS) int numTaskManagersConnected,
			@JsonProperty(FIELD_NAME_SLOTS_TOTAL) int numSlotsTotal,
			@JsonProperty(FIELD_NAME_SLOTS_AVAILABLE) int numSlotsAvailable,
			@JsonProperty(FIELD_NAME_JOBS_RUNNING) int numJobsRunningOrPending,
			@JsonProperty(FIELD_NAME_JOBS_FINISHED) int numJobsFinished,
			@JsonProperty(FIELD_NAME_JOBS_CANCELLED) int numJobsCancelled,
			@JsonProperty(FIELD_NAME_JOBS_FAILED) int numJobsFailed,
			@JsonProperty(FIELD_NAME_VERSION) String version,
			@JsonProperty(FIELD_NAME_COMMIT) String commitId) {
		super(
			numTaskManagersConnected,
			numSlotsTotal,
			numSlotsAvailable,
			numJobsRunningOrPending,
			numJobsFinished,
			numJobsCancelled,
			numJobsFailed);

		this.version = Preconditions.checkNotNull(version);
		this.commitId = Preconditions.checkNotNull(commitId);
	}

	public ClusterOverviewWithVersion(
			int numTaskManagersConnected,
			int numSlotsTotal,
			int numSlotsAvailable,
			JobsOverview jobs1,
			JobsOverview jobs2,
			String version,
			String commitId) {
		super(numTaskManagersConnected, numSlotsTotal, numSlotsAvailable, jobs1, jobs2);

		this.version = Preconditions.checkNotNull(version);
		this.commitId = Preconditions.checkNotNull(commitId);
	}

	public static ClusterOverviewWithVersion fromStatusOverview(ClusterOverview statusOverview, String version, String commitId) {
		return new ClusterOverviewWithVersion(
			statusOverview.getNumTaskManagersConnected(),
			statusOverview.getNumSlotsTotal(),
			statusOverview.getNumSlotsAvailable(),
			statusOverview.getNumJobsRunningOrPending(),
			statusOverview.getNumJobsFinished(),
			statusOverview.getNumJobsCancelled(),
			statusOverview.getNumJobsFailed(),
			version,
			commitId);
	}

	public String getVersion() {
		return version;
	}

	public String getCommitId() {
		return commitId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		ClusterOverviewWithVersion that = (ClusterOverviewWithVersion) o;

		return Objects.equals(version, that.getVersion()) && Objects.equals(commitId, that.getCommitId());
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (version != null ? version.hashCode() : 0);
		result = 31 * result + (commitId != null ? commitId.hashCode() : 0);
		return result;
	}
}
