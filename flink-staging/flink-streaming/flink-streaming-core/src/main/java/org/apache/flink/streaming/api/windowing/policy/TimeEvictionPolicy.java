/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.policy;

import java.util.LinkedList;

import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

/**
 * This eviction policy evicts all elements which are older then a specified
 * time. The time is measured using a given {@link Timestamp} implementation. A
 * point in time is always represented as long. Therefore, the granularity can
 * be set as long value as well.
 * 
 * @param <DATA>
 *            The type of the incoming data points which are processed by this
 *            policy.
 */
public class TimeEvictionPolicy<DATA> implements ActiveEvictionPolicy<DATA>,
		CloneableEvictionPolicy<DATA> {

	/**
	 * auto generated version id
	 */
	private static final long serialVersionUID = -1457476766124518220L;

	private long granularity;
	private TimestampWrapper<DATA> timestampWrapper;
	private LinkedList<Long> buffer = new LinkedList<Long>();

	/**
	 * This eviction policy evicts all elements which are older than a specified
	 * time. The time is measured using a given {@link Timestamp}
	 * implementation. A point in time is always represented as long. Therefore,
	 * the granularity can be set as long value as well. If this value is set to
	 * 2 the policy will evict all elements which are older as 2.
	 * 
	 * <code>
	 *   while (time(firstInBuffer)<current time-granularity){
	 *   	evict firstInBuffer;
	 *   }
	 * </code>
	 * 
	 * @param granularity
	 *            The granularity of the eviction. If this value is set to 2 the
	 *            policy will evict all elements which are older as 2(if
	 *            (time(X)<current time-granularity) evict X).
	 * @param timestampWrapper
	 *            The {@link TimestampWrapper} to measure the time with. This
	 *            can be either user defined of provided by the API.
	 */
	public TimeEvictionPolicy(long granularity, TimestampWrapper<DATA> timestampWrapper) {
		this.timestampWrapper = timestampWrapper;
		this.granularity = granularity;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int notifyEvictionWithFakeElement(Object datapoint, int bufferSize) {
		checkForDeleted(bufferSize);

		long threshold;
		try {
			threshold = (Long) datapoint - granularity;
		} catch (ClassCastException e) {
			threshold = timestampWrapper.getTimestamp((DATA) datapoint) - granularity;
		}

		// return result
		return deleteAndCountExpired(threshold);

	}

	@Override
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize) {

		checkForDeleted(bufferSize);

		// remember timestamp
		long time = timestampWrapper.getTimestamp(datapoint);

		// delete and count expired tuples
		long threshold = time - granularity;
		int counter = deleteAndCountExpired(threshold);

		// Add current element to buffer
		buffer.add(time);

		// return result
		return counter;

	}

	private void checkForDeleted(int bufferSize) {
		// check for deleted tuples (deletes by other policies)
		while (bufferSize < this.buffer.size()) {
			this.buffer.removeFirst();
		}
	}

	private int deleteAndCountExpired(long threshold) {
		int counter = 0;
		while (!buffer.isEmpty()) {

			if (buffer.getFirst() <= threshold) {
				buffer.removeFirst();
				counter++;
			} else {
				break;
			}
		}
		return counter;

	}

	@Override
	public TimeEvictionPolicy<DATA> clone() {
		return new TimeEvictionPolicy<DATA>(granularity, timestampWrapper);
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof TimeEvictionPolicy)) {
			return false;
		} else {
			try {
				@SuppressWarnings("unchecked")
				TimeEvictionPolicy<DATA> otherPolicy = (TimeEvictionPolicy<DATA>) other;
				return granularity == otherPolicy.granularity
						&& timestampWrapper.equals(otherPolicy.timestampWrapper);
			} catch (ClassCastException e) {
				return false;
			}
		}
	}

	public long getWindowSize() {
		return granularity;
	}

	@Override
	public String toString() {
		return "TimePolicy(" + granularity + ", " + timestampWrapper.getClass().getSimpleName()
				+ ")";
	}

	public TimestampWrapper<DATA> getTimeStampWrapper() {
		return timestampWrapper;
	}

}
