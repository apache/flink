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

import java.util.Optional;

/**
 * A {@link Trigger} that can turn any {@link Trigger} into a timeout {@code Trigger}.
 *
 * <p>Each element arriving will emit a ProcessingTimeTimer withing the interval,
 * you can control if the timer will be reset for each new event arriving,
 * by the resetTimerOnNewRecord flag.
 * you can control if the state will be cleared after reach the timeout, by the
 * shouldClearAtTimeout flag.
 *
 * @param <T> The type of elements on which this trigger can operate.
 * @param <W> The type of {@link Window} on which this trigger can operate.
 */

@PublicEvolving
public class ProcessingTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {

	private static final long serialVersionUID = 1L;

	private Trigger<T, W> nestedTrigger;
	private final long interval;
	private final boolean resetTimerOnNewRecord;
	private final boolean shouldClearAtTimeout;

	private final ValueStateDescriptor<Long> timeoutStateDesc;

	private ProcessingTimeoutTrigger(Trigger<T, W> nestedTrigger, long interval, boolean resetTimerOnNewRecord, boolean shouldClearAtTimeout) {
		this.nestedTrigger = nestedTrigger;
		this.interval = interval;
		this.resetTimerOnNewRecord = resetTimerOnNewRecord;
		this.shouldClearAtTimeout = shouldClearAtTimeout;
		this.timeoutStateDesc = new ValueStateDescriptor<Long>("timeout", LongSerializer.INSTANCE);
	}

	@Override
	public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
		TriggerResult triggerResult = this.nestedTrigger.onElement(element, timestamp, window, ctx);
		if (triggerResult.isFire()) {
			this.clear(window, ctx);
			return triggerResult;
		}

		ValueState<Long> timeoutState = ctx.getPartitionedState(this.timeoutStateDesc);
		long nextFireTimestamp = ctx.getCurrentProcessingTime() + this.interval;
		Optional<Long> timeoutTimestamp = Optional.ofNullable(timeoutState.value());
		if (timeoutTimestamp.isPresent() && resetTimerOnNewRecord) {
			ctx.deleteProcessingTimeTimer(timeoutTimestamp.get());
			timeoutState.clear();
			timeoutTimestamp = Optional.empty();
		}

		if (timeoutTimestamp.isEmpty()) {
			timeoutState.update(nextFireTimestamp);
			ctx.registerProcessingTimeTimer(nextFireTimestamp);
		}

		return triggerResult;
	}

	@Override
	public TriggerResult onProcessingTime(long timestamp, W window, TriggerContext ctx) throws Exception {
		TriggerResult triggerResult = this.nestedTrigger.onProcessingTime(timestamp, window, ctx);
		if (shouldClearAtTimeout) {
			this.clear(window, ctx);
		}
		return triggerResult.isPurge() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
	}

	@Override
	public TriggerResult onEventTime(long timestamp, W window, TriggerContext ctx) throws Exception {
		TriggerResult triggerResult = this.nestedTrigger.onEventTime(timestamp, window, ctx);
		if (shouldClearAtTimeout) {
			this.clear(window, ctx);
		}
		return triggerResult.isPurge() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
	}

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		ValueState<Long> timeoutTimestampState = ctx.getPartitionedState(this.timeoutStateDesc);
		Optional<Long> timeoutTimestamp = Optional.ofNullable(timeoutTimestampState.value());
		if (timeoutTimestamp.isPresent()) {
			ctx.deleteProcessingTimeTimer(timeoutTimestamp.get());
			timeoutTimestampState.clear();
		}
		this.nestedTrigger.clear(window, ctx);
	}

	@Override
	public String toString() {
		return "TimeoutTrigger(" + this.nestedTrigger.toString() + ")";
	}

	/**
	 * Creates a trigger that fire when the inner trigger is fired or when timeout is over, the first of both.
	 *
	 * for example:
	 * ProcessingTimeoutTrigger.of(CountTrigger.of(3),100), will create a CountTrigger with timeout of 100 millis.
	 * So, if the first record arriving at t time, and the second record arriving at t+50 time,
	 * the trigger will be fire when the third record will be arriving or when t+100 (our interval timeout) will be arriving.
	 *
	 * @param nestedTrigger the trigger to apply the {@link ProcessingTimeoutTrigger} on.
	 * @param interval      the time in milliseconds to apply the timer when element arrive.
	 * @param <T>           The type of the element.
	 * @param <W>           The type of {@link Window Windows} on which this trigger can operate.
	 * @return {@link ProcessingTimeoutTrigger} with the above configuration.
	 */
	public static <T, W extends Window> ProcessingTimeoutTrigger<T, W> of(Trigger<T, W> nestedTrigger, long interval) {
		return new ProcessingTimeoutTrigger<>(nestedTrigger, interval, false, true);
	}

	/**
	 * Creates a trigger that fire when the inner trigger is fired or when timeout is over, the first of both.
	 *
	 * for example:
	 * ProcessingTimeoutTrigger.of(CountTrigger.of(3),100,false,true), will create a CountTrigger with timeout of 100 millis.
	 * So, if the first record arriving at t time, and the second record arriving at t+50 time,
	 * the trigger will be fire when the third record will be arriving or when t+100 (our interval timeout) will be arriving.
	 *
	 * @param nestedTrigger         the trigger to apply the {@link ProcessingTimeoutTrigger} on.
	 * @param interval              the time in milliseconds to apply the timer when element arrive.
	 * @param resetTimerOnNewRecord each time new element arrive, reset the timer and start a new one.
	 * @param shouldClearAtTimeout  when timeout occurs and onProcessingTime is called, should clear the state of the {@param nestedTrigger}.
	 * @param <T>                   The type of the element.
	 * @param <W>                   The type of {@link Window Windows} on which this trigger can operate.
	 * @return {@link ProcessingTimeoutTrigger} with the above configuration.
	 */
	public static <T, W extends Window> ProcessingTimeoutTrigger<T, W> of(Trigger<T, W> nestedTrigger, long interval, boolean resetTimerOnNewRecord, boolean shouldClearAtTimeout) {
		return new ProcessingTimeoutTrigger<>(nestedTrigger, interval, resetTimerOnNewRecord, shouldClearAtTimeout);
	}

}
