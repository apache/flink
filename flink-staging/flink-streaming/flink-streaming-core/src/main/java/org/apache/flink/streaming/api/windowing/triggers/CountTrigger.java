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

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;

/**
 * A {@link Trigger} that fires once the count of elements in a pane reaches the given count.
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
public class CountTrigger<W extends Window> implements Trigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private final long maxCount;

	private CountTrigger(long maxCount) {
		this.maxCount = maxCount;
	}

	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws IOException {
		OperatorState<Long> count = ctx.getKeyValueState("count", 0L);
		long currentCount = count.value() + 1;
		count.update(currentCount);
		if (currentCount >= maxCount) {
			count.update(0L);
			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onTime(long time, TriggerContext ctx) {
		return null;
	}

	@Override
	public String toString() {
		return "CountTrigger(" +  maxCount + ")";
	}

	/**
	 * Creates a trigger that fires once the number of elements in a pane reaches the given count.
	 *
	 * @param maxCount The count of elements at which to fire.
	 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
	 */
	public static <W extends Window> CountTrigger<W> of(long maxCount) {
		return new CountTrigger<>(maxCount);
	}
}
