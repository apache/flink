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
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A sample of stack traces for one or more tasks.
 *
 * <p>The sampling is triggered in {@link StackTraceSampleCoordinator}.
 */
public class StackTraceSample {

	/** ID of this sample (unique per job). */
	private final int sampleId;

	/** Time stamp, when the sample was triggered. */
	private final long startTime;

	/** Time stamp, when all stack traces were collected at the JobManager. */
	private final long endTime;

	/** Map of stack traces by execution ID. */
	private final Map<ExecutionAttemptID, List<StackTraceElement[]>> stackTracesByTask;

	/**
	 * Creates a stack trace sample.
	 *
	 * @param sampleId          ID of the sample.
	 * @param startTime         Time stamp, when the sample was triggered.
	 * @param endTime           Time stamp, when all stack traces were
	 *                          collected at the JobManager.
	 * @param stackTracesByTask Map of stack traces by execution ID.
	 */
	public StackTraceSample(
			int sampleId,
			long startTime,
			long endTime,
			Map<ExecutionAttemptID, List<StackTraceElement[]>> stackTracesByTask) {

		checkArgument(sampleId >= 0, "Negative sample ID");
		checkArgument(startTime >= 0, "Negative start time");
		checkArgument(endTime >= startTime, "End time before start time");

		this.sampleId = sampleId;
		this.startTime = startTime;
		this.endTime = endTime;
		this.stackTracesByTask = Collections.unmodifiableMap(stackTracesByTask);
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
	 * Returns the time stamp, when all stack traces were collected at the
	 * JobManager.
	 *
	 * @return Time stamp, when all stack traces were collected at the
	 * JobManager
	 */
	public long getEndTime() {
		return endTime;
	}

	/**
	 * Returns the a map of stack traces by execution ID.
	 *
	 * @return Map of stack traces by execution ID
	 */
	public Map<ExecutionAttemptID, List<StackTraceElement[]>> getStackTraces() {
		return stackTracesByTask;
	}

	@Override
	public String toString() {
		return "StackTraceSample{" +
				"sampleId=" + sampleId +
				", startTime=" + startTime +
				", endTime=" + endTime +
				'}';
	}
}
