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

import java.util.LinkedList;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Non-grouped pre-reducer for sliding eviction policy
 * (the slide size is smaller than the window size).
 */
public abstract class SlidingPreReducer<T> extends WindowBuffer<T> implements PreAggregator {

	private static final long serialVersionUID = 1L;

	protected ReduceFunction<T> reducer;

	protected T currentReduced;
	protected LinkedList<T> reduced = new LinkedList<T>();
	protected LinkedList<Integer> elementsPerPreAggregate = new LinkedList<Integer>();

	protected TypeSerializer<T> serializer;

	protected int toRemove = 0;

	protected int elementsSinceLastPreAggregate = 0;

	public SlidingPreReducer(ReduceFunction<T> reducer, TypeSerializer<T> serializer) {
		this.reducer = reducer;
		this.serializer = serializer;
	}

	public void emitWindow(Collector<StreamRecord<StreamWindow<T>>> collector) {
		StreamWindow<T> currentWindow = createEmptyWindow();

		try {
			if (addFinalAggregate(currentWindow) || emitEmpty) {
				collector.collect(new StreamRecord<StreamWindow<T>>(currentWindow));
			} 
			afterEmit();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	protected void afterEmit() {
		// Do nothing by default
	}

	public boolean addFinalAggregate(StreamWindow<T> currentWindow) throws Exception {
		T finalReduce = null;

		if (!reduced.isEmpty()) {
			finalReduce = reduced.get(0);
			for (int i = 1; i < reduced.size(); i++) {
				finalReduce = reducer.reduce(finalReduce, serializer.copy(reduced.get(i)));

			}
			if (currentReduced != null) {
				finalReduce = reducer.reduce(finalReduce, serializer.copy(currentReduced));
			}

		} else {
			finalReduce = currentReduced;
		}

		if (finalReduce != null) {
			currentWindow.add(finalReduce);
			return true;
		} else {
			return false;
		}

	}

	public void store(T element) throws Exception {
		addToBufferIfEligible(element);
		afterStore();
	}

	protected void afterStore() {
		// Do nothing by default
	}

	protected void addToBufferIfEligible(T element) throws Exception {
		if (currentEligible(element) && currentNotEmpty()) {
			addCurrentToBuffer(element);
			elementsPerPreAggregate.add(elementsSinceLastPreAggregate);
			elementsSinceLastPreAggregate = 0;
			resetCurrent();
		}
		updateCurrent(element);

		elementsSinceLastPreAggregate++;
	}

	protected void resetCurrent() {
		currentReduced = null;
	}

	protected boolean currentNotEmpty() {
		return currentReduced != null;
	}

	protected void updateCurrent(T element) throws Exception {
		if (currentReduced == null) {
			currentReduced = element;
		} else {
			currentReduced = reducer.reduce(serializer.copy(currentReduced), element);
		}
	}

	protected void addCurrentToBuffer(T element) throws Exception {
		reduced.add(currentReduced);
	}

	protected abstract boolean currentEligible(T next);

	public void evict(int n) {
		toRemove += n;

		Integer lastPreAggregateSize = elementsPerPreAggregate.peek();
		while (lastPreAggregateSize != null && lastPreAggregateSize <= toRemove) {
			toRemove = max(toRemove - elementsPerPreAggregate.removeFirst(), 0);
			removeLastReduced();
			lastPreAggregateSize = elementsPerPreAggregate.peek();
		}

		if (lastPreAggregateSize == null) {
			toRemove = 0;
		}
	}

	protected void removeLastReduced() {
		reduced.removeFirst();
	}

	public static int max(int a, int b) {
		if (a > b) {
			return a;
		} else {
			return b;
		}
	}

	@Override
	public abstract SlidingPreReducer<T> clone();

	@Override
	public String toString() {
		return currentReduced.toString();
	}

}
