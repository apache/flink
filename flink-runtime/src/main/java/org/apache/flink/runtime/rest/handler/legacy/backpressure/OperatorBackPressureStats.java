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

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Back pressure statistics of multiple tasks.
 *
 * <p>Statistics are gathered by sampling stack traces of running tasks. The
 * back pressure ratio denotes the ratio of traces indicating back pressure
 * to the total number of sampled traces.
 */
public class OperatorBackPressureStats implements Serializable {

	private static final long serialVersionUID = 1L;

	/** ID of the corresponding sample. */
	private final int sampleId;

	/** End time stamp of the corresponding sample. */
	private final long endTimestamp;

	/** Back pressure ratio per subtask. */
	private final double[] subTaskBackPressureRatio;

	/** Maximum back pressure ratio. */
	private final double maxSubTaskBackPressureRatio;

	public OperatorBackPressureStats(
			int sampleId,
			long endTimestamp,
			double[] subTaskBackPressureRatio) {

		this.sampleId = sampleId;
		this.endTimestamp = endTimestamp;
		this.subTaskBackPressureRatio = checkNotNull(subTaskBackPressureRatio, "Sub task back pressure ratio");
		checkArgument(subTaskBackPressureRatio.length >= 1, "No Sub task back pressure ratio specified");

		double max = 0;
		for (double ratio : subTaskBackPressureRatio) {
			if (ratio > max) {
				max = ratio;
			}
		}

		maxSubTaskBackPressureRatio = max;
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
	 * Returns the time stamp, when all stack traces were collected at the
	 * JobManager.
	 *
	 * @return Time stamp, when all stack traces were collected at the
	 * JobManager
	 */
	public long getEndTimestamp() {
		return endTimestamp;
	}

	/**
	 * Returns the number of sub tasks.
	 *
	 * @return Number of sub tasks.
	 */
	public int getNumberOfSubTasks() {
		return subTaskBackPressureRatio.length;
	}

	/**
	 * Returns the ratio of stack traces indicating back pressure to total
	 * number of sampled stack traces.
	 *
	 * @param index Subtask index.
	 *
	 * @return Ratio of stack traces indicating back pressure to total number
	 * of sampled stack traces.
	 */
	public double getBackPressureRatio(int index) {
		return subTaskBackPressureRatio[index];
	}

	/**
	 * Returns the maximum back pressure ratio of all sub tasks.
	 *
	 * @return Maximum back pressure ratio of all sub tasks.
	 */
	public double getMaxBackPressureRatio() {
		return maxSubTaskBackPressureRatio;
	}

	@Override
	public String toString() {
		return "OperatorBackPressureStats{" +
				"sampleId=" + sampleId +
				", endTimestamp=" + endTimestamp +
				", subTaskBackPressureRatio=" + Arrays.toString(subTaskBackPressureRatio) +
				'}';
	}
}
