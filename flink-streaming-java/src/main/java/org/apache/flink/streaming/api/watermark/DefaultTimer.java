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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Keeping track of in-flight timers
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class DefaultTimer<K, W extends Window> implements WindowTimer<K, W> {
	private long timestamp;
	private K key;
	private W window;

	public DefaultTimer(long timestamp, K key, W window) {
		this.timestamp = timestamp;
		this.key = key;
		this.window = window;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()){
			return false;
		}

		WindowTimer<?, ?> timer = (WindowTimer<?, ?>) o;

		return timestamp == timer.getTimestamp()
			&& key.equals(timer.getKey())
			&& window.equals(timer.getWindow());

	}

	@Override
	public int hashCode() {
		int result = (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + key.hashCode();
		result = 31 * result + window.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "WindowTimer{" +
			"timestamp=" + timestamp +
			", key=" + key +
			", window=" + window +
			'}';
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public W getWindow() {
		return window;
	}

	@Override
	public int compareTo(WindowTimer<K, W> timer) {
		return Long.compare(this.getTimestamp(), timer.getTimestamp());
	}
}
