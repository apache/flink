/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A {@link Trigger} that continuously fires based on a given time interval. This fires based
 * on {@link org.apache.flink.streaming.api.watermark.Watermark Watermarks}.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
@PublicEvolving
public class ContinuousEventTimeTrigger<W extends Window> extends Trigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private final long interval;

	/** When merging we take the lowest of all fire timestamps as the new fire timestamp. */
	private final ReducingStateDescriptor<Long> stateDesc =
			new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

	private ContinuousEventTimeTrigger(long interval) {
		this.interval = interval;
	}

	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {

		ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);

		if (fireTimestamp.get() == null) {
			long start = timestamp - (timestamp % interval);
			long nextFireTimestamp = start + interval;

			ctx.registerEventTimeTimer(nextFireTimestamp);

			fireTimestamp.add(nextFireTimestamp);
			return TriggerResult.CONTINUE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
		ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);

		if (fireTimestamp.get().equals(time)) {
			fireTimestamp.clear();
			fireTimestamp.add(time + interval);
			ctx.registerEventTimeTimer(time + interval);
			return TriggerResult.FIRE;

		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public void onFire(Window window, TriggerContext ctx) throws Exception {
		// do nothing.
		//
		// Even if we fire at the end or not, the state has already been cleared in the onEventTime(),
		// so there is nothing to do here. We do the cleanup in the onEventTime, as before, and not here,
		// as in other triggers. This is because i) we assume that existing triggers are not combinable,
		// so whenever this trigger proposes to fire, it will fire and ii) if somebody in the future tries
		// to combine this with another trigger, then cleaning the state onEventTime gives the
		// opportunity to the trigger to reset the timer for a future potential firing. If not, then the
		// timer would be stuck to the same initial value, as in the onEventTime we only check for equality,
		// and onElement we check if the state is null.
	}

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
		long timestamp = fireTimestamp.get();
		ctx.deleteEventTimeTimer(timestamp);
		fireTimestamp.clear();
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	public TriggerResult onMerge(W window, OnMergeContext ctx) {
		ctx.mergePartitionedState(stateDesc);
		return TriggerResult.CONTINUE;
	}

	@Override
	public String toString() {
		return "ContinuousEventTimeTrigger(" + interval + ")";
	}

	@VisibleForTesting
	public long getInterval() {
		return interval;
	}

	/**
	 * Creates a trigger that continuously fires based on the given interval.
	 *
	 * @param interval The time interval at which to fire.
	 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
	 */
	public static <W extends Window> ContinuousEventTimeTrigger<W> of(Time interval) {
		return new ContinuousEventTimeTrigger<>(interval.toMilliseconds());
	}

	private static class Min implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return Math.min(value1, value2);
		}
	}
}
