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
import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;

public class WindowUtils {

	public enum WindowTransformation {
		REDUCEWINDOW, MAPWINDOW, NONE;
		private Function UDF;

		public WindowTransformation with(Function UDF) {
			this.UDF = UDF;
			return this;
		}

		public Function getUDF() {
			return UDF;
		}
	}

	public static boolean isParallelPolicy(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		return (eviction instanceof CountEvictionPolicy && (trigger instanceof CountTriggerPolicy || trigger instanceof TimeTriggerPolicy))
				|| (eviction instanceof TumblingEvictionPolicy && trigger instanceof CountTriggerPolicy);
	}

	public static boolean isTimeOnly(TriggerPolicy<?> trigger, EvictionPolicy<?> eviction) {
		return trigger instanceof TimeTriggerPolicy && eviction instanceof TimeEvictionPolicy;
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
}
