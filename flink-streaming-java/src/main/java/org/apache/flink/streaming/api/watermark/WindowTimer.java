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
 * Allows to customize the watermark timer system
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@PublicEvolving
public interface WindowTimer<K, W extends Window> extends Comparable<WindowTimer<K, W>> {
	/**
	 * The type of key associated with the watermark timer
	 * @return the key type
	 */
	K getKey() ;

	/**
	 * The timestamp at which the timer was created
	 * @return the timestamp
	 */
	long getTimestamp();

	/**
	 * The window associated with this timer
	 * @return the window associated with this timer
	 */
	W getWindow();

	@Override
	int compareTo(WindowTimer<K, W> timer);
}
