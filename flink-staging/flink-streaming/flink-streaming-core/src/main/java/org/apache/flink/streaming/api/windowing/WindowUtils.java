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

package org.apache.flink.streaming.api.windowing;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.KeepAllEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;

/**
 * Utility class that contains helper methods to work with stream windowing.
 */
public class WindowUtils {

	public enum WindowTransformation {
		REDUCEWINDOW, MAPWINDOW, FOLDWINDOW, NONE;
		private Function UDF;

		public WindowTransformation with(Function UDF) {
			this.UDF = UDF;
			return this;
		}

		public Function getUDF() {
			return UDF;
		}
	}

	public static boolean isParallelPolicy(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction,
			int parallelism) {
		return ((eviction instanceof CountEvictionPolicy && (trigger instanceof CountTriggerPolicy || trigger instanceof TimeTriggerPolicy))
				|| (eviction instanceof TumblingEvictionPolicy && trigger instanceof CountTriggerPolicy) || (WindowUtils
				.isTimeOnly(trigger, eviction) && parallelism > 1));
	}

	public static boolean isSlidingTimePolicy(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		if (isTimeOnly(trigger, eviction)) {
			long slide = getSlideSize(trigger);
			long window = getWindowSize(eviction);

			return slide < window
					&& getTimeStampWrapper(trigger).equals(getTimeStampWrapper(eviction));
		} else {
			return false;
		}
	}

	public static boolean isSlidingCountPolicy(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		if (isCountOnly(trigger, eviction)) {
			long slide = getSlideSize(trigger);
			long window = getWindowSize(eviction);

			return slide < window
					&& ((CountTriggerPolicy<?>) trigger).getStart() == ((CountEvictionPolicy<?>) eviction)
							.getStart()
					&& ((CountEvictionPolicy<?>) eviction).getDeleteOnEviction() == 1;
		} else {
			return false;
		}
	}

	public static <X> TimestampWrapper<X> getTimeStampWrapper(TriggerPolicy<X> trigger) {
		if (trigger instanceof TimeTriggerPolicy) {
			return ((TimeTriggerPolicy<X>) trigger).getTimeStampWrapper();
		} else {
			throw new IllegalArgumentException(
					"Timestamp wrapper can only be accessed for time policies");
		}
	}

	public static <X> TimestampWrapper<X> getTimeStampWrapper(EvictionPolicy<X> eviction) {
		if (eviction instanceof EvictionPolicy) {
			return ((TimeEvictionPolicy<X>) eviction).getTimeStampWrapper();
		} else {
			throw new IllegalArgumentException(
					"Timestamp wrapper can only be accessed for time policies");
		}
	}

	public static long getSlideSize(TriggerPolicy<?> trigger) {
		if (trigger instanceof TimeTriggerPolicy) {
			return ((TimeTriggerPolicy<?>) trigger).getSlideSize();
		} else if (trigger instanceof CountTriggerPolicy) {
			return ((CountTriggerPolicy<?>) trigger).getSlideSize();
		} else {
			throw new IllegalArgumentException(
					"Slide size can only be accessed for time or count policies");
		}
	}

	public static long getWindowSize(EvictionPolicy<?> eviction) {
		if (eviction instanceof TimeEvictionPolicy) {
			return ((TimeEvictionPolicy<?>) eviction).getWindowSize();
		} else if (eviction instanceof CountEvictionPolicy) {
			return ((CountEvictionPolicy<?>) eviction).getWindowSize();
		} else {
			throw new IllegalArgumentException(
					"Window size can only be accessed for time or count policies");
		}
	}

	public static boolean isTumblingPolicy(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		if (eviction instanceof TumblingEvictionPolicy || eviction instanceof KeepAllEvictionPolicy) {
			return true;
		} else if (isTimeOnly(trigger, eviction)) {
			long slide = getSlideSize(trigger);
			long window = getWindowSize(eviction);

			return slide == window
					&& getTimeStampWrapper(trigger).equals(getTimeStampWrapper(eviction));
		} else if (isCountOnly(trigger, eviction)) {
			long slide = getSlideSize(trigger);
			long window = getWindowSize(eviction);

			return slide == window
					&& ((CountTriggerPolicy<?>) trigger).getStart() == ((CountEvictionPolicy<?>) eviction)
							.getStart()
					&& ((CountEvictionPolicy<?>) eviction).getDeleteOnEviction() == 1;
		} else {
			return false;
		}
	}

	public static boolean isTimeOnly(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		return trigger instanceof TimeTriggerPolicy
				&& (eviction instanceof TimeEvictionPolicy || eviction instanceof KeepAllEvictionPolicy);
	}

	public static boolean isCountOnly(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		return trigger instanceof CountTriggerPolicy && eviction instanceof CountEvictionPolicy;
	}

	public static boolean isSystemTimeTrigger(TriggerPolicy<?> trigger) {
		return trigger instanceof TimeTriggerPolicy
				&& ((TimeTriggerPolicy<?>) trigger).timestampWrapper.isDefaultTimestamp();
	}

	public static class WindowKey<R> implements KeySelector<StreamWindow<R>, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer getKey(StreamWindow<R> value) throws Exception {
			return value.windowID;
		}

	}

	public static boolean isJumpingCountPolicy(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		if (isCountOnly(trigger, eviction)) {
			long slide = getSlideSize(trigger);
			long window = getWindowSize(eviction);

			return slide > window
					&& ((CountTriggerPolicy<?>) trigger).getStart() == ((CountEvictionPolicy<?>) eviction)
							.getStart()
					&& ((CountEvictionPolicy<?>) eviction).getDeleteOnEviction() == 1;
		} else {
			return false;
		}
	}

	public static boolean isJumpingTimePolicy(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		if (isTimeOnly(trigger, eviction)) {
			long slide = getSlideSize(trigger);
			long window = getWindowSize(eviction);

			return slide > window
					&& getTimeStampWrapper(trigger).equals(getTimeStampWrapper(eviction));
		} else {
			return false;
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private WindowUtils() {
		throw new RuntimeException();
	}
}
