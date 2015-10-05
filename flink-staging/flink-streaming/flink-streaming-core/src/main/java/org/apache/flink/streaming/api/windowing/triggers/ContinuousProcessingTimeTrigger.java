/**
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.streaming.api.windowing.time.AbstractTime;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A {@link Trigger} that continuously fires based on a given time interval. The time is the current
 * system time.
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
public class ContinuousProcessingTimeTrigger<W extends Window> implements Trigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private long interval;

	private long nextFireTimestamp = 0;

	private ContinuousProcessingTimeTrigger(long interval) {
		this.interval = interval;
	}

	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) {
		long currentTime = System.currentTimeMillis();
		if (nextFireTimestamp == 0) {
			long start = currentTime - (currentTime % interval);
			nextFireTimestamp = start + interval;

			ctx.registerProcessingTimeTimer(nextFireTimestamp);
			return TriggerResult.CONTINUE;
		}
		if (currentTime > nextFireTimestamp) {
			long start = currentTime - (currentTime % interval);
			nextFireTimestamp = start + interval;

			ctx.registerProcessingTimeTimer(nextFireTimestamp);

			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onTime(long time, TriggerContext ctx) {
		// only fire if an element didn't already fire
		long currentTime = System.currentTimeMillis();
		if (currentTime > nextFireTimestamp) {
			long start = currentTime - (currentTime % interval);
			nextFireTimestamp = start + interval;
			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public Trigger<Object, W> duplicate() {
		return new ContinuousProcessingTimeTrigger<>(interval);
	}

	@VisibleForTesting
	public long getInterval() {
		return interval;
	}

	@Override
	public String toString() {
		return "ContinuousProcessingTimeTrigger(" + interval + ")";
	}

	/**
	 * Creates a trigger that continuously fires based on the given interval.
	 *
	 * @param interval The time interval at which to fire.
	 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
	 */
	public static <W extends Window> ContinuousProcessingTimeTrigger<W> of(AbstractTime interval) {
		return new ContinuousProcessingTimeTrigger<>(interval.toMilliseconds());
	}
}
