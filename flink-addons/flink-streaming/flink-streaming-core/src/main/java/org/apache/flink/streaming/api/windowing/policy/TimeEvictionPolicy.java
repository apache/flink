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

import org.apache.flink.streaming.api.invokable.util.TimeStamp;

/**
 * This eviction policy evicts all elements which are older then a specified
 * time. The time is measured using a given {@link TimeStamp} implementation. A
 * point in time is always represented as long. Therefore, the granularity can
 * be set as long value as well.
 *
 * @param <DATA>
 *            The type of the incoming data points which are processed by this
 *            policy.
 */
public class TimeEvictionPolicy<DATA> implements ActiveEvictionPolicy<DATA> {

	/**
	 * auto generated version id
	 */
	private static final long serialVersionUID = -1457476766124518220L;

	private long granularity;
	private TimeStamp<DATA> timestamp;
	private LinkedList<DATA> buffer = new LinkedList<DATA>();

	/**
	 * This eviction policy evicts all elements which are older than a specified
	 * time. The time is measured using a given {@link TimeStamp}
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
	 * @param timestamp
	 *            The {@link TimeStamp} to measure the time with. This can be
	 *            either user defined of provided by the API.
	 */
	public TimeEvictionPolicy(long granularity, TimeStamp<DATA> timestamp) {
		this.timestamp = timestamp;
		this.granularity = granularity;
	}

	@Override
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize) {
		return notifyEviction(datapoint, triggered, bufferSize, false);
	}

	@Override
	public int notifyEvictionWithFakeElement(DATA datapoint, int bufferSize) {
		return notifyEviction(datapoint, true, bufferSize, true);
	}

	private int notifyEviction(DATA datapoint, boolean triggered, int bufferSize, boolean isFake) {
		// check for deleted tuples (deletes by other policies)
		while (bufferSize < this.buffer.size()) {
			this.buffer.removeFirst();
		}

		// delete and count expired tuples
		int counter = 0;
		while (!buffer.isEmpty()) {

			if (timestamp.getTimestamp(buffer.getFirst()) < timestamp.getTimestamp(datapoint)
					- granularity) {
				buffer.removeFirst();
				counter++;
			} else {
				break;
			}
		}

		if (!isFake) {
			// Add current element to buffer
			buffer.add(datapoint);
		}

		// return result
		return counter;
	}

}
