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

package org.apache.flink.streaming.api.windowing.evictors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * An {@link Evictor} that keeps up to a certain amount of elements.
 *
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
@PublicEvolving
public class CountEvictor<W extends Window> implements Evictor<Object, W> {
	private static final long serialVersionUID = 1L;

	private final long maxCount;
	private final boolean doEvictAfter;

	private CountEvictor(long count, boolean doEvictAfter) {
		this.maxCount = count;
		this.doEvictAfter = doEvictAfter;
	}

	private CountEvictor(long count) {
		this.maxCount = count;
		this.doEvictAfter = false;
	}

	@Override
	public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
		if (!doEvictAfter) {
			evict(elements, size, ctx);
		}
	}

	@Override
	public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
		if (doEvictAfter) {
			evict(elements, size, ctx);
		}
	}

	private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
		if (size <= maxCount) {
			return;
		} else {
			int evictedCount = 0;
			for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext();){
				iterator.next();
				evictedCount++;
				if (evictedCount > size - maxCount) {
					break;
				} else {
					iterator.remove();
				}
			}
		}
	}

	/**
	 * Creates a {@code CountEvictor} that keeps the given number of elements.
	 * Eviction is done before the window function.
	 *
	 * @param maxCount The number of elements to keep in the pane.
	 */
	public static <W extends Window> CountEvictor<W> of(long maxCount) {
		return new CountEvictor<>(maxCount);
	}

	/**
	 * Creates a {@code CountEvictor} that keeps the given number of elements in the pane
	 * Eviction is done before/after the window function based on the value of doEvictAfter.
	 *
	 * @param maxCount The number of elements to keep in the pane.
	 * @param doEvictAfter Whether to do eviction after the window function.
     */
	public static <W extends Window> CountEvictor<W> of(long maxCount, boolean doEvictAfter) {
		return new CountEvictor<>(maxCount, doEvictAfter);
	}
}
