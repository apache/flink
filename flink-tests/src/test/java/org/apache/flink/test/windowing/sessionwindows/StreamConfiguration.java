/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.windowing.sessionwindows;

/**
 * Configuration for event streams
 */
public final class StreamConfiguration {

	// allowed lateness for this stream (in ms)
	private final long allowedLateness;

	// how many late events within lateness per session the stream will contain
	private final int lateEventsWithinLateness;

	// how many late events after lateness per session the stream will contain
	private final int lateEventsAfterLateness;

	// hint for the maximum additional gap used to in between two sessions
	private final long maxAdditionalSessionGap;

	public StreamConfiguration(long allowedLateness,
	                           int lateEventsWithinLateness,
	                           int lateEventsAfterLateness,
	                           long maxAdditionalSessionGap) {
		this.allowedLateness = allowedLateness;
		this.lateEventsWithinLateness = lateEventsWithinLateness;
		this.lateEventsAfterLateness = lateEventsAfterLateness;
		this.maxAdditionalSessionGap = maxAdditionalSessionGap;
	}

	public long getAllowedLateness() {
		return allowedLateness;
	}

	public int getLateEventsWithinLateness() {
		return lateEventsWithinLateness;
	}

	public int getLateEventsAfterLateness() {
		return lateEventsAfterLateness;
	}

	public long getMaxAdditionalSessionGap() {
		return maxAdditionalSessionGap;
	}

	public static StreamConfiguration of(long allowedLateness,
	                                     int lateEventsPerSessionWithinLateness,
	                                     int lateEventsPerSessionOutsideLateness,
	                                     long maxAdditionalSessionGap) {
		return new StreamConfiguration(
				allowedLateness,
				lateEventsPerSessionWithinLateness,
				lateEventsPerSessionOutsideLateness,
				maxAdditionalSessionGap);
	}

	@Override
	public String toString() {
		return "StreamConfiguration{" +
				"allowedLateness=" + allowedLateness +
				", lateEventsWithinLateness=" + lateEventsWithinLateness +
				", lateEventsAfterLateness=" + lateEventsAfterLateness +
				", maxAdditionalSessionGap=" + maxAdditionalSessionGap +
				'}';
	}
}