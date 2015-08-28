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

package org.apache.flink.streaming.api.windowing.helper;

import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * This helper represents a time based count or eviction policy. By default the
 * time is measured with {@link System#currentTimeMillis()} in
 * {@link SystemTimestamp}.
 * 
 * @param <DATA>
 *            The data type which is handled by the time stamp used in the
 *            policy represented by this helper
 */
public class Time<DATA> extends WindowingHelper<DATA> {

	protected long length;
	protected TimeUnit granularity;
	protected TimestampWrapper<DATA> timestampWrapper;
	protected long delay;

	/**
	 * Creates a helper representing a trigger which triggers every given
	 * length or an eviction which evicts all elements older than length.
	 * 
	 * @param length
	 *            The number of time units
	 * @param timeUnit
	 *            The unit of time such as minute oder millisecond. Note that
	 *            the smallest possible granularity is milliseconds. Any smaller
	 *            time unit might cause an error at runtime due to conversion
	 *            problems.
	 * @param timestamp
	 *            The user defined timestamp that will be used to extract time
	 *            information from the incoming elements
	 * @param startTime
	 *            The startTime of the stream for computing the first window
	 */
	private Time(long length, TimeUnit timeUnit, Timestamp<DATA> timestamp, long startTime) {
		this(length, timeUnit, new TimestampWrapper<DATA>(timestamp, startTime));
	}

	/**
	 * Creates a helper representing a trigger which triggers every given
	 * length or an eviction which evicts all elements older than length.
	 * 
	 * @param length
	 *            The number of time units
	 * @param timeUnit
	 *            The unit of time such as minute oder millisecond. Note that
	 *            the smallest possible granularity is milliseconds. Any smaller
	 *            time unit might cause an error at runtime due to conversion
	 *            problems.
	 * @param timestampWrapper
	 *            The user defined {@link TimestampWrapper} that will be used to
	 *            extract time information from the incoming elements
	 */
	private Time(long length, TimeUnit timeUnit, TimestampWrapper<DATA> timestampWrapper) {
		this.length = length;
		this.granularity = timeUnit;
		this.timestampWrapper = timestampWrapper;
	}

	@Override
	public EvictionPolicy<DATA> toEvict() {
		return new TimeEvictionPolicy<DATA>(granularityInMillis(), timestampWrapper);
	}

	@Override
	public TriggerPolicy<DATA> toTrigger() {
		return new TimeTriggerPolicy<DATA>(granularityInMillis(), timestampWrapper);
	}

	/**
	 * Creates a helper representing a time trigger which triggers every given
	 * length (slide size) or a time eviction which evicts all elements older
	 * than length (window size) using System time.
	 * 
	 * @param length
	 *            The number of time units
	 * @param timeUnit
	 *            The unit of time such as minute oder millisecond. Note that
	 *            the smallest possible granularity is milliseconds. Any smaller
	 *            time unit might cause an error at runtime due to conversion
	 *            problems.
	 * @return Helper representing the time based trigger and eviction policy
	 */
	@SuppressWarnings("unchecked")
	public static <DATA> Time<DATA> of(long length, TimeUnit timeUnit) {
		return new Time<DATA>(length, timeUnit,
				(TimestampWrapper<DATA>) SystemTimestamp.getWrapper());
	}

	/**
	 * Creates a helper representing a time trigger which triggers every given
	 * length (slide size) or a time eviction which evicts all elements older
	 * than length (window size) using a user defined timestamp extractor.
	 * 
	 * @param length
	 *            The number of time units
	 * @param timestamp
	 *            The user defined timestamp that will be used to extract time
	 *            information from the incoming elements
	 * @param startTime
	 *            The startTime used to compute the first window
	 * @return Helper representing the time based trigger and eviction policy
	 */
	public static <DATA> Time<DATA> of(long length, Timestamp<DATA> timestamp, long startTime) {
		return new Time<DATA>(length, null, timestamp, startTime);
	}

	/**
	 * Creates a helper representing a time trigger which triggers every given
	 * length (slide size) or a time eviction which evicts all elements older
	 * than length (window size) using a user defined timestamp extractor. By
	 * default the start time is set to 0.
	 * 
	 * @param length
	 *            The number of time units
	 * @param timestamp
	 *            The user defined timestamp that will be used to extract time
	 *            information from the incoming elements
	 * @return Helper representing the time based trigger and eviction policy
	 */
	public static <DATA> Time<DATA> of(long length, Timestamp<DATA> timestamp) {
		return of(length, timestamp, 0);
	}
	
	protected long granularityInMillis() {
		return granularity == null ? length : granularity.toMillis(length);
	}
}
