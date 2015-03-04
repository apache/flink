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
import org.apache.flink.util.Collector;

public abstract class SlidingPreReducer<T> implements WindowBuffer<T>, CompletePreAggregator {

	private static final long serialVersionUID = 1L;

	protected ReduceFunction<T> reducer;

	protected T currentReduced;
	protected LinkedList<T> reduced = new LinkedList<T>();
	protected LinkedList<Integer> elementsPerPreAggregate = new LinkedList<Integer>();

	protected TypeSerializer<T> serializer;

	protected int index = 0;
	protected int toRemove = 0;

	protected int elementsSinceLastPreAggregate = 0;

	public SlidingPreReducer(ReduceFunction<T> reducer, TypeSerializer<T> serializer) {
		this.reducer = reducer;
		this.serializer = serializer;
	}

	public boolean emitWindow(Collector<StreamWindow<T>> collector) {
		StreamWindow<T> currentWindow = new StreamWindow<T>();
		T finalAggregate = getFinalAggregate();
		if (finalAggregate != null) {
			currentWindow.add(finalAggregate);
			collector.collect(currentWindow);
			updateIndexAtEmit();
			return true;
		} else {
			updateIndexAtEmit();
			return false;
		}

	}

	protected abstract void updateIndexAtEmit();

	public T getFinalAggregate() {
		try {
			if (!reduced.isEmpty()) {
				T finalReduce = reduced.get(0);
				for (int i = 1; i < reduced.size(); i++) {
					finalReduce = reducer.reduce(finalReduce, serializer.copy(reduced.get(i)));

				}
				if (currentReduced != null) {
					finalReduce = reducer.reduce(finalReduce, serializer.copy(currentReduced));
				}
				return finalReduce;
			} else {
				return currentReduced;
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void store(T element) throws Exception {
		addToBufferIfEligible(element);
		index++;
	}

	protected void addToBufferIfEligible(T element) throws Exception {
		if (addCurrentToReduce(element) && currentReduced != null) {
			reduced.add(currentReduced);
			elementsPerPreAggregate.add(elementsSinceLastPreAggregate);
			currentReduced = element;
			elementsSinceLastPreAggregate = 1;
		} else {
			if (currentReduced == null) {
				currentReduced = element;
			} else {
				currentReduced = reducer.reduce(serializer.copy(currentReduced), element);
			}
			elementsSinceLastPreAggregate++;
		}
	}

	protected abstract boolean addCurrentToReduce(T next);

	public void evict(int n) {
		toRemove += n;

		Integer lastPreAggregateSize = elementsPerPreAggregate.peek();
		while (lastPreAggregateSize != null && lastPreAggregateSize <= toRemove) {
			toRemove = max(toRemove - elementsPerPreAggregate.removeFirst(), 0);
			reduced.removeFirst();
			lastPreAggregateSize = elementsPerPreAggregate.peek();
		}

		if (lastPreAggregateSize == null) {
			toRemove = 0;
		}
	}

	public int max(int a, int b) {
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
