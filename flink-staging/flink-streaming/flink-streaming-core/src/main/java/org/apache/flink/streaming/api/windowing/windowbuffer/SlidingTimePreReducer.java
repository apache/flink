/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.windowbuffer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

/**
 * Non-grouped pre-reducer for sliding time eviction policy
 * (the policies are based on time, and the slide size is smaller than the window size).
 */
public class SlidingTimePreReducer<T> extends SlidingPreReducer<T> {

	private static final long serialVersionUID = 1L;

	private long windowSize;
	private long slideSize;
	private TimestampWrapper<T> timestampWrapper;
	private T lastStored;
	protected long windowStartTime;

	public SlidingTimePreReducer(ReduceFunction<T> reducer, TypeSerializer<T> serializer,
			long windowSize, long slideSize, TimestampWrapper<T> timestampWrapper) {
		super(reducer, serializer);
		if (windowSize > slideSize) {
			this.windowSize = windowSize;
			this.slideSize = slideSize;
		} else {
			throw new RuntimeException(
					"Window size needs to be larger than slide size for the sliding pre-reducer");
		}
		this.timestampWrapper = timestampWrapper;
		this.windowStartTime = timestampWrapper.getStartTime();
	}

	@Override
	public void store(T element) throws Exception {
		super.store(element);
		lastStored = element;
	}

	@Override
	public SlidingTimePreReducer<T> clone() {
		return new SlidingTimePreReducer<T>(reducer, serializer, windowSize, slideSize,
				timestampWrapper);
	}

	@Override
	public String toString() {
		return currentReduced.toString();
	}

	@Override
	protected void afterEmit() {
		long lastTime = timestampWrapper.getTimestamp(lastStored);
		if (lastTime - windowStartTime >= slideSize) {
			windowStartTime = windowStartTime + slideSize;
		}
	}

	@Override
	public void evict(int n) {
		toRemove += n;
		Integer lastPreAggregateSize = elementsPerPreAggregate.peek();

		while (lastPreAggregateSize != null && lastPreAggregateSize <= toRemove) {
			toRemove = max(toRemove - elementsPerPreAggregate.removeFirst(), 0);
			reduced.removeFirst();
			lastPreAggregateSize = elementsPerPreAggregate.peek();
		}

		if (toRemove > 0 && lastPreAggregateSize == null) {
			currentReduced = null;
			toRemove = 0;
		}
	}

	@Override
	protected boolean currentEligible(T next) {
		return windowStartTime == timestampWrapper.getStartTime()
				|| timestampWrapper.getTimestamp(next) - windowStartTime >= slideSize;
	}
}
