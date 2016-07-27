/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.watermark;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * An interface when used with User-defined functions allows to get a callback
 * when a watermark is processed by the {@code WindowOperator}}
 * <p>It also allows to implement a watermark timer system that user-defined functions
 * can use. See {@code WindowTimer} and {@code  DefaultTimer} for the default timer implementation</p>
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@PublicEvolving
public interface EventTimeFunction<K, W extends Window> {
	/**
	 * Invoked when a watermark is processed by the watermark
	 * timer system
	 * @param watermark
	 */
	void onWatermark(Watermark watermark);

	/**
	 * Creates a watermark timer system
	 * @param timestamp
	 * @param key
	 * @param window
	 * @return a watermark timer system {@code WindowTimer}
	 */
	WindowTimer createTimer(long timestamp, K key, W window);
}
