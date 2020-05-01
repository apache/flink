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

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.time.Duration;

/**
 * A {@link Trigger} that can turn any {@link Trigger} into a timeout {@code Trigger}.
 *
 * <p>On the first arriving element a configurable processing-time timeout will be set. Using
 * {@link
 * #of(Trigger, Duration, boolean, boolean)}, you can also re-new the timer for each arriving
 * element by specifying {@code resetTimerOnNewRecord} and you can specify whether {@link
 * Trigger#clear(Window, TriggerContext)} should be called on timout via {@code
 * shouldClearOnTimeout}.
 *
 * @param <T> The type of elements on which this trigger can operate.
 * @param <W> The type of {@link Window} on which this trigger can operate.
 */

@PublicEvolving
public class ProcessingTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {

	private static final long serialVersionUID = 1L;

	private final Trigger<T, W> nestedTrigger;
	private final long interval;
	private final boolean resetTimerOnNewRecord;
	private final boolean shouldClearOnTimeout;

	private final ValueStateDescriptor<Long> timeoutStateDesc;

	private ProcessingTimeoutTrigger(
			Trigger<T, W> nestedTrigger,
			long interval,
			boolean resetTimerOnNewRecord,
			boolean shouldClearOnTimeout) {
		this.nestedTrigger = nestedTrigger;
		this.interval = interval;
		this.resetTimerOnNewRecord = resetTimerOnNewRecord;
		this.shouldClearOnTimeout = shouldClearOnTimeout;
		this.timeoutStateDesc = new ValueStateDescriptor<>("timeout", LongSerializer.INSTANCE);
	}

	@Override
	public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx)
			throws Exception {
		TriggerResult triggerResult = this.nestedTrigger.onElement(element, timestamp, window, ctx);
		if (triggerResult.isFire()) {
			this.clear(window, ctx);
			return triggerResult;
		}

		ValueState<Long> timeoutState = ctx.getPartitionedState(this.timeoutStateDesc);
		long nextFireTimestamp = ctx.getCurrentProcessingTime() + this.interval;
		Long timeoutTimestamp = timeoutState.value();
		if (timeoutTimestamp != null && resetTimerOnNewRecord) {
			ctx.deleteProcessingTimeTimer(timeoutTimestamp);
			timeoutState.clear();
			timeoutTimestamp = null;
		}

		if (timeoutTimestamp == null) {
			timeoutState.update(nextFireTimestamp);
			ctx.registerProcessingTimeTimer(nextFireTimestamp);
		}

		return triggerResult;
	}

	@Override
	public TriggerResult onProcessingTime(long timestamp, W window, TriggerContext ctx)
			throws Exception {
		TriggerResult triggerResult = this.nestedTrigger.onProcessingTime(timestamp, window, ctx);
		if (shouldClearOnTimeout) {
			this.clear(window, ctx);
		}
		return triggerResult.isPurge() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
	}

	@Override
	public TriggerResult onEventTime(long timestamp, W window, TriggerContext ctx)
			throws Exception {
		TriggerResult triggerResult = this.nestedTrigger.onEventTime(timestamp, window, ctx);
		if (shouldClearOnTimeout) {
			this.clear(window, ctx);
		}
		return triggerResult.isPurge() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
	}

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		ValueState<Long> timeoutTimestampState = ctx.getPartitionedState(this.timeoutStateDesc);
		Long timeoutTimestamp = timeoutTimestampState.value();
		if (timeoutTimestamp != null) {
			ctx.deleteProcessingTimeTimer(timeoutTimestamp);
			timeoutTimestampState.clear();
		}
		this.nestedTrigger.clear(window, ctx);
	}

	@Override
	public String toString() {
		return "TimeoutTrigger(" + this.nestedTrigger.toString() + ")";
	}

	/**
	 * Creates a new {@link ProcessingTimeoutTrigger} that fires when the inner trigger is fired or
	 * when the timeout timer fires.
	 *
	 * <p>For example:
	 * {@code ProcessingTimeoutTrigger.of(CountTrigger.of(3), 100)}, will create a CountTrigger with
	 * timeout of 100 millis. So, if the first record arrives at time {@code t}, and the second
	 * record arrives at time {@code t+50 }, the trigger will fire when the third record arrives or
	 * when the time is {code t+100} (timeout).
	 *
	 * @param nestedTrigger the nested {@link Trigger}
	 * @param timeout the timeout interval
	 *
	 * @return {@link ProcessingTimeoutTrigger} with the above configuration.
	 */
	public static <T, W extends Window> ProcessingTimeoutTrigger<T, W> of(
			Trigger<T, W> nestedTrigger,
			Duration timeout) {
		return new ProcessingTimeoutTrigger<>(nestedTrigger, timeout.toMillis(), false, true);
	}

	/**
	 * Creates a new {@link ProcessingTimeoutTrigger} that fires when the inner trigger is fired or
	 * when the timeout timer fires.
	 *
	 * <p>For example:
	 * {@code ProcessingTimeoutTrigger.of(CountTrigger.of(3), 100, false, true)}, will create a
	 * CountTrigger with timeout of 100 millis. So, if the first record arrives at time {@code t},
	 * and the second record arrives at time {@code t+50 }, the trigger will fire when the third
	 * record arrives or when the time is {code t+100} (timeout).
	 *
	 * @param nestedTrigger the nested {@link Trigger}
	 * @param timeout the timeout interval
	 * @param resetTimerOnNewRecord each time a new element arrives, reset the timer and start a
	 * 		new one
	 * @param shouldClearOnTimeout whether to call {@link Trigger#clear(Window, TriggerContext)}
	 * 		when the processing-time timer fires
	 * @param <T> The type of the element.
	 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
	 *
	 * @return {@link ProcessingTimeoutTrigger} with the above configuration.
	 */
	public static <T, W extends Window> ProcessingTimeoutTrigger<T, W> of(
			Trigger<T, W> nestedTrigger,
			Duration timeout,
			boolean resetTimerOnNewRecord,
			boolean shouldClearOnTimeout) {
		return new ProcessingTimeoutTrigger<>(
				nestedTrigger, timeout.toMillis(), resetTimerOnNewRecord, shouldClearOnTimeout);
	}

}
