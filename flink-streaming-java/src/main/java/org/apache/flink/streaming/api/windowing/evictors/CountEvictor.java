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

import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * An {@link Evictor} that keeps only a certain amount of elements.
 *
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
public class CountEvictor<W extends Window> implements Evictor<Object, W> {
	private static final long serialVersionUID = 1L;

	private final long maxCount;

	private CountEvictor(long count) {
		this.maxCount = count;
	}

	@Override
	public int evict(Iterable<StreamRecord<Object>> elements, int size, W window) {
		if (size > maxCount) {
			return (int) (size - maxCount);
		} else {
			return 0;
		}
	}

	/**
	 * Creates a {@code CountEvictor} that keeps the given number of elements.
	 *
	 * @param maxCount The number of elements to keep in the pane.
	 */
	public static <W extends Window> CountEvictor<W> of(long maxCount) {
		return new CountEvictor<>(maxCount);
	}
}
