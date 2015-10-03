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

import org.apache.flink.streaming.api.windowing.windows.Window;
import scala.Serializable;

/**
 * A {@code Trigger} determines when a pane of a window should be evaluated to emit the
 * results for that part of the window.
 *
 * <p>
 * A pane is the bucket of elements that have the same key (assigned by the
 * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
 * be in multiple panes of it was assigned to multiple windows by the
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
 * have their own instance of the {@code Trigger}.
 *
 * @param <T> The type of elements on which this {@code Trigger} works.
 * @param <W> The type of {@link Window Windows} on which this {@code Trigger} can operate.
 */
public interface Trigger<T, W extends Window> extends Serializable {

	/**
	 * Called for every element that gets added to a pane. The result of this will determine
	 * whether the pane is evaluated to emit results.
	 *
	 * @param element The element that arrived.
	 * @param timestamp The timestamp of the element that arrived.
	 * @param window The window to which this pane belongs.
	 * @param ctx A context object that can be used to register timer callbacks.
	 */
	TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx);

	/**
	 * Called when a timer that was set using the trigger context fires.
	 *
	 * @param time The timestamp at which the timer fired.
	 * @param ctx A context object that can be used to register timer callbacks.
	 */
	TriggerResult onTime(long time, TriggerContext ctx);

	/**
	 * Creates a duplicate of the {@code Trigger} without the state of the original {@code Trigger}.
	 * @return The duplicate {@code Trigger} object.
	 */
	Trigger<T, W> duplicate();

	/**
	 * Result type for trigger methods. This determines what happens which the window.
	 *
	 * <p>
	 * On {@code FIRE} the pane is evaluated and results are emitted. The contents of the window
	 * are kept. {@code FIRE_AND_PURGE} acts like {@code FIRE} but the contents of the pane
	 * are purged. On {@code CONTINUE} nothing happens, processing continues.
	 */
	enum TriggerResult {
		CONTINUE, FIRE_AND_PURGE, FIRE
	}

	/**
	 * A context object that is given to {@code Trigger} methods to allow them to register timer
	 * callbacks.
	 */
	interface TriggerContext {

		/**
		 * Register a system time callback. When the current system time passes the specified
		 * time {@link #onTime(long, TriggerContext)} is called.
		 *
		 * @param time The time at which to invoke {@link #onTime(long, TriggerContext)}
		 */
		void registerProcessingTimeTimer(long time);

		/**
		 * Register a watermark callback. When the current watermark passes the specified
		 * time {@link #onTime(long, TriggerContext)} is called.
		 *
		 * @see org.apache.flink.streaming.api.watermark.Watermark
		 *
		 * @param time The watermark at which to invoke {@link #onTime(long, TriggerContext)}
		 */
		void registerWatermarkTimer(long time);
	}
}
