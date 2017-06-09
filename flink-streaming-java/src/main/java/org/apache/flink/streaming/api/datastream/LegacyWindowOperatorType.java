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

package org.apache.flink.streaming.api.datastream;

/**
 * For specifying what type of window operator was used to create the state
 * that a {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator}
 * is restoring from. This is used to signal that state written using an aligned processing-time
 * window operator should be restored.
 */
public enum LegacyWindowOperatorType {

	FAST_ACCUMULATING(true, false),

	FAST_AGGREGATING(false, true),

	NONE(false, false);

	// ------------------------------------------------------------------------

	private final boolean fastAccumulating;
	private final boolean fastAggregating;

	LegacyWindowOperatorType(boolean fastAccumulating, boolean fastAggregating) {
		this.fastAccumulating = fastAccumulating;
		this.fastAggregating = fastAggregating;
	}

	public boolean isFastAccumulating() {
		return fastAccumulating;
	}

	public boolean isFastAggregating() {
		return fastAggregating;
	}

	@Override
	public String toString() {
		if (fastAccumulating) {
			return "AccumulatingProcessingTimeWindowOperator";
		} else if (fastAggregating) {
			return "AggregatingProcessingTimeWindowOperator";
		} else {
			return "WindowOperator";
		}
	}
}
