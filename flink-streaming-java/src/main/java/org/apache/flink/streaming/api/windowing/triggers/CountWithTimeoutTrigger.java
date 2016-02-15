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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;

/**
 * A {@link Trigger} that fires once the number of elements in a pane reaches the given count or the timeout expires, whichever happens first.
 *
 * @param <T> The type of elements.
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
public class CountWithTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {

	private static final long serialVersionUID = 1L;

	private final long maxCount;
	private final long timeoutMs;

	private final ValueStateDescriptor<Long> countDesc = new ValueStateDescriptor<>("count", LongSerializer.INSTANCE, 0L);
	private final ValueStateDescriptor<Long> deadlineDesc = new ValueStateDescriptor<>("deadline", LongSerializer.INSTANCE, Long.MAX_VALUE);

	private CountWithTimeoutTrigger(long maxCount, long timeoutMs) {
		this.maxCount = maxCount;
		this.timeoutMs = timeoutMs;
	}

	@Override
	public TriggerResult onElement(T element, long timestamp, W window, Trigger.TriggerContext ctx) throws IOException {
		final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
		final ValueState<Long> count = ctx.getPartitionedState(countDesc);

		final long currentDeadline = deadline.value();
		final long currentTimeMs = System.currentTimeMillis();

		final long newCount = count.value() + 1;

		if (currentTimeMs >= currentDeadline || newCount >= maxCount) {
			return fire(deadline, count);
		}

		if (currentDeadline == deadlineDesc.getDefaultValue()) {
			final long nextDeadline = currentTimeMs + timeoutMs;
			deadline.update(nextDeadline);
			ctx.registerProcessingTimeTimer(nextDeadline);
		}

		count.update(newCount);

		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onEventTime(long time, W window, Trigger.TriggerContext ctx) {
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, W window, Trigger.TriggerContext ctx) throws Exception {
		final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
		// fire only if the deadline hasn't changed since registering this timer
		if (deadline.value() == time) {
			return fire(deadline, ctx.getPartitionedState(countDesc));
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		final ValueState<Long> deadline = ctx.getPartitionedState(deadlineDesc);
		final long deadlineValue = deadline.value();
		if (deadlineValue != deadlineDesc.getDefaultValue()) {
			ctx.deleteProcessingTimeTimer(deadlineValue);
		}
		deadline.clear();
	}

	private TriggerResult fire(ValueState<Long> deadline, ValueState<Long> count) throws IOException {
		deadline.update(Long.MAX_VALUE);
		count.update(0L);
		return TriggerResult.FIRE;
	}

	@Override
	public String toString() {
		return "CountWithTimeoutTrigger(" + maxCount + "," + timeoutMs + ")";
	}

	public static <T, W extends Window> CountWithTimeoutTrigger<T, W> of(long maxCount, long intervalMs) {
		return new CountWithTimeoutTrigger<>(maxCount, intervalMs);
	}

}
