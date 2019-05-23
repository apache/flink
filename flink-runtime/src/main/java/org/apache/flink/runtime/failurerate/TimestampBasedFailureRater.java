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

package org.apache.flink.runtime.failurerate;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.util.clock.Clock;

import java.util.ArrayDeque;

/**
 * A timestamp queue based failure rater implementation.
 */
public class TimestampBasedFailureRater implements FailureRater {
	private static final int DEFAULT_TIMESTAMP_SIZE = 300;
	private static final int MILLISECOND_PER_SECOND = 1000;
	private final int maximumFailureRate;
	private final Time failureInterval;
	private final ArrayDeque<Long> failureTimestamps;
	private int count;

	public TimestampBasedFailureRater(int maximumFailureRate, Time failureInterval) {
		this.maximumFailureRate = maximumFailureRate;
		this.failureInterval = failureInterval;
		this.failureTimestamps = new ArrayDeque<>(maximumFailureRate > 0 ? maximumFailureRate : DEFAULT_TIMESTAMP_SIZE);
		this.count = 0;
	}

	@Override
	public void recordFailure(Clock clock) {
		failureTimestamps.add(clock.absoluteTimeMillis());
		count++;
	}

	@Override
	public int getMaximumFailureRate() {
		return maximumFailureRate;
	}

	@Override
	public Time getFailureInterval() {
		return failureInterval;
	}

	@Override
	public double getCurrentFailureRate() {
		Long currentTimeStamp = System.currentTimeMillis();
		while (!failureTimestamps.isEmpty() &&
			currentTimeStamp - failureTimestamps.peek() > failureInterval.toMilliseconds()) {
			failureTimestamps.remove();
		}

		return failureTimestamps.size();
	}

	@Override
	public boolean exceedFailureRate() {
		if (maximumFailureRate < 0) {
			return false;
		}

		double currentRate = getCurrentFailureRate();
		if (currentRate < maximumFailureRate) {
			return  false;
		}

		Long earliestTimestamp = failureTimestamps.peek();

		return System.currentTimeMillis() - earliestTimestamp < failureInterval.toMilliseconds();
	}

	@Override
	public void markEvent() {
		failureTimestamps.add(System.currentTimeMillis());
	}

	@Override
	public void markEvent(long n) {
		failureTimestamps.add(n);
		count++;
	}

	public double getRate() {
		return (getCurrentFailureRate() / failureInterval.toMilliseconds()) * MILLISECOND_PER_SECOND;
	}

	@Override
	public long getCount() {
		return count;
	}
}
