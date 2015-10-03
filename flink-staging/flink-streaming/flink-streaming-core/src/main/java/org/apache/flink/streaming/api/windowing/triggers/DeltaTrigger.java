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

import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A {@link Trigger} that fires based on a {@link DeltaFunction} and a threshold.
 *
 * <p>
 * This trigger calculates a delta between the data point which triggered last
 * and the currently arrived data point. It triggers if the delta is higher than
 * a specified threshold.
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
public class DeltaTrigger<T, W extends Window> implements Trigger<T, W> {
	private static final long serialVersionUID = 1L;

	DeltaFunction<T> deltaFunction;
	private double threshold;
	private transient T lastElement;

	private DeltaTrigger(double threshold, DeltaFunction<T> deltaFunction) {
		this.deltaFunction = deltaFunction;
		this.threshold = threshold;
	}

	@Override
	public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) {
		if (lastElement == null) {
			lastElement = element;
			return TriggerResult.CONTINUE;
		}
		if (deltaFunction.getDelta(lastElement, element) > this.threshold) {
			lastElement = element;
			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onTime(long time, TriggerContext ctx) {
		return null;
	}

	@Override
	public Trigger<T, W> duplicate() {
		return new DeltaTrigger<>(threshold, deltaFunction);
	}

	@Override
	public String toString() {
		return "DeltaTrigger(" +  deltaFunction + ", " + threshold + ")";
	}

	/**
	 * Creates a delta trigger from the given threshold and {@code DeltaFunction}.
	 *
	 * @param threshold The threshold at which to trigger.
	 * @param deltaFunction The delta function to use
	 *
	 * @param <T> The type of elements on which this trigger can operate.
	 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
	 * @return
	 */
	public static <T, W extends Window> DeltaTrigger<T, W> of(double threshold, DeltaFunction<T> deltaFunction) {
		return new DeltaTrigger<>(threshold, deltaFunction);
	}
}
