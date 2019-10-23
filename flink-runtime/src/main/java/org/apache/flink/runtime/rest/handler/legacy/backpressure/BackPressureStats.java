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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Task back pressure stats for one or more tasks.
 *
 * <p>The stats are calculated from sampling triggered in {@link BackPressureSampleCoordinator}.
 */
public class BackPressureStats {

	/** ID of the sample (unique per job). */
	private final int sampleId;

	/** Time stamp, when the sample was triggered. */
	private final long startTime;

	/** Time stamp, when all back pressure stats were collected at the JobManager. */
	private final long endTime;

	/** Map of back pressure ratio by execution ID. */
	private final Map<ExecutionAttemptID, Double> backPressureRatioByTask;

	/**
	 * Creates a task back pressure sample.
	 *
	 * @param sampleId                ID of the sample.
	 * @param startTime               Time stamp, when the sample was triggered.
	 * @param endTime                 Time stamp, when all back pressure stats
	 *                                were collected at the JobManager.
	 * @param backPressureRatioByTask Map of back pressure stats by execution ID.
	 */
	public BackPressureStats(
			int sampleId,
			long startTime,
			long endTime,
			Map<ExecutionAttemptID, Double> backPressureRatioByTask) {

		checkArgument(sampleId >= 0, "Negative sample ID");
		checkArgument(startTime >= 0, "Negative start time");
		checkArgument(endTime >= startTime, "End time before start time");

		this.sampleId = sampleId;
		this.startTime = startTime;
		this.endTime = endTime;
		this.backPressureRatioByTask = Collections.unmodifiableMap(backPressureRatioByTask);
	}

	/**
	 * Returns the ID of the sample.
	 *
	 * @return ID of the sample
	 */
	public int getSampleId() {
		return sampleId;
	}

	/**
	 * Returns the time stamp, when the sample was triggered.
	 *
	 * @return Time stamp, when the sample was triggered
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * Returns the time stamp, when all back pressure stats were collected at
	 * the JobManager.
	 *
	 * @return Time stamp, when all back pressure stats were collected at the
	 * JobManager
	 */
	public long getEndTime() {
		return endTime;
	}

	/**
	 * Returns the a map of back pressure ratio by execution ID.
	 *
	 * @return Map of back pressure ratio by execution ID
	 */
	public Map<ExecutionAttemptID, Double> getBackPressureRatioByTask() {
		return backPressureRatioByTask;
	}

	@Override
	public String toString() {
		return "TaskBackPressureStats{" +
				"sampleId=" + sampleId +
				", startTime=" + startTime +
				", endTime=" + endTime +
				'}';
	}
}
