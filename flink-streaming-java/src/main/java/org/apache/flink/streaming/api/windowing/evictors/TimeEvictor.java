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
package org.apache.flink.streaming.api.windowing.evictors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.windowing.time.AbstractTime;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * An {@link Evictor} that keeps elements for a certain amount of time. Elements older
 * than {@code current_time - keep_time} are evicted.
 *
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
public class TimeEvictor<W extends Window> implements Evictor<Object, W> {
	private static final long serialVersionUID = 1L;

	private final long windowSize;

	public TimeEvictor(long windowSize) {
		this.windowSize = windowSize;
	}

	@Override
	public int evict(Iterable<StreamRecord<Object>> elements, int size, W window) {
		int toEvict = 0;
		long currentTime = Iterables.getLast(elements).getTimestamp();
		long evictCutoff = currentTime - windowSize;
		for (StreamRecord<Object> record: elements) {
			if (record.getTimestamp() > evictCutoff) {
				break;
			}
			toEvict++;
		}
		return toEvict;
	}

	@Override
	public String toString() {
		return "TimeEvictor(" + windowSize + ")";
	}

	@VisibleForTesting
	public long getWindowSize() {
		return windowSize;
	}

	/**
	 * Creates a {@code TimeEvictor} that keeps the given number of elements.
	 *
	 * @param windowSize The amount of time for which to keep elements.
	 */
	public static <W extends Window> TimeEvictor<W> of(AbstractTime windowSize) {
		return new TimeEvictor<>(windowSize.toMilliseconds());
	}
}
