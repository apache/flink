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

import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.windowing.extractor.Extractor;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * This helper represents a time based count or eviction policy. By default the
 * time is measured with {@link System#currentTimeMillis()} in
 * {@link DefaultTimeStamp}.
 * 
 * @param <DATA>
 *            The data type which is handled by the time stamp used in the
 *            policy represented by this helper
 */
public class Time<DATA> implements WindowingHelper<DATA> {

	private int timeVal;
	private TimeUnit granularity;
	private Extractor<Long, DATA> longToDATAExtractor;
	private TimeStamp<DATA> timeStamp;
	private long delay;

	/**
	 * Creates an helper representing a trigger which triggers every given
	 * timeVal or an eviction which evicts all elements older than timeVal.
	 * 
	 * @param timeVal
	 *            The number of time units
	 * @param timeUnit
	 *            The unit of time such as minute oder millisecond. Note that
	 *            the smallest possible granularity is milliseconds. Any smaller
	 *            time unit might cause an error at runtime due to conversion
	 *            problems.
	 */
	private Time(int timeVal, TimeUnit timeUnit) {
		this.timeVal = timeVal;
		this.granularity = timeUnit;
		this.longToDATAExtractor = new NullExtractor<DATA>();
		this.timeStamp = new DefaultTimeStamp<DATA>();
		this.delay = 0;
	}

	@Override
	public EvictionPolicy<DATA> toEvict() {
		return new TimeEvictionPolicy<DATA>(granularityInMillis(), timeStamp);
	}

	@Override
	public TriggerPolicy<DATA> toTrigger() {
		return new TimeTriggerPolicy<DATA>(granularityInMillis(), timeStamp, delay,
				longToDATAExtractor);
	}

	/**
	 * Creates an helper representing a trigger which triggers every given
	 * timeVal or an eviction which evicts all elements older than timeVal.
	 * 
	 * @param timeVal
	 *            The number of time units
	 * @param granularity
	 *            The unit of time such as minute oder millisecond. Note that
	 *            the smallest possible granularity is milliseconds. Any smaller
	 *            time unit might cause an error at runtime due to conversion
	 *            problems.
	 * @return an helper representing a trigger which triggers every given
	 *         timeVal or an eviction which evicts all elements older than
	 *         timeVal.
	 */
	public static <DATA> Time<DATA> of(int timeVal, TimeUnit granularity) {
		return new Time<DATA>(timeVal, granularity);
	}

	@SuppressWarnings("unchecked")
	public <R> Time<R> withTimeStamp(TimeStamp<R> timeStamp, Extractor<Long, R> extractor) {
		this.timeStamp = (TimeStamp<DATA>) timeStamp;
		this.longToDATAExtractor = (Extractor<Long, DATA>) extractor;
		return (Time<R>) this;
	}

	public Time<DATA> withDelay(long delay) {
		this.delay = delay;
		return this;
	}

	private long granularityInMillis() {
		return this.granularity.toMillis(this.timeVal);
	}

	public static class NullExtractor<T> implements Extractor<Long, T> {

		private static final long serialVersionUID = 1L;

		@Override
		public T extract(Long in) {
			return null;
		}

	}

}
