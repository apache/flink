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

/**
 * Non-grouped pre-reducer for tumbling eviction policy.
 */
public class SlidingCountPreReducer<T> extends SlidingPreReducer<T> {

	private static final long serialVersionUID = 1L;

	private int windowSize;
	private int slideSize;
	private int start;

	public SlidingCountPreReducer(ReduceFunction<T> reducer, TypeSerializer<T> serializer,
			int windowSize, int slideSize, int start) {
		super(reducer, serializer);
		if (windowSize > slideSize) {
			this.windowSize = windowSize;
			this.slideSize = slideSize;
			this.start = start;
		} else {
			throw new RuntimeException(
					"Window size needs to be larger than slide size for the sliding pre-reducer");
		}
		index = index - start;
	}

	@Override
	public SlidingCountPreReducer<T> clone() {
		return new SlidingCountPreReducer<T>(reducer, serializer, windowSize, slideSize, start);
	}

	@Override
	public void store(T element) throws Exception {
		if (index >= 0) {
			super.store(element);
		} else {
			index++;
		}
	}

	@Override
	public String toString() {
		return currentReduced.toString();
	}

	@Override
	protected boolean addCurrentToReduce(T next) {
		if (index <= slideSize) {
			return true;
		} else {
			return index == windowSize;
		}
	}

	@Override
	protected void updateIndexAtEmit() {
		if (index >= slideSize) {
			index = index - slideSize;
		}
	}

}
