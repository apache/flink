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

package org.apache.flink.contrib.streaming.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.windowbuffer.GenericGroupablePreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.util.FieldAccessor;

import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.io.Serializable;

/**
 * Non-grouped pre-reducer for calculating median with any eviction policy.
 */
public class MedianPreReducer<T> extends GenericGroupablePreReducer<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	// This is for getting and setting the field specified in the parameters to the median call.
	private FieldAccessor<T, Double> fieldAccessor;

	private TypeSerializer<T> serializer;

	// These contain lower-than-median and higher-than-median elements.
	// Elements that compare equal to the median can be in either.
	// The invariant is that low always contains the same amount or one more element than high.
	//   (see the assert in updateMedian)
	TreeMultiset<T>
			low = new TreeMultiset<T>(new CompareOnField()),
			high = new TreeMultiset<T>(new CompareOnField());
	Deque<T> elements = new ArrayDeque<T>();

	T median;

	public T getAggregate() {
		return median;
	}

	public MedianPreReducer(FieldAccessor<T, Double> fieldAccessor, TypeSerializer<T> serializer) {
		this.fieldAccessor = fieldAccessor;
		this.serializer = serializer;
	}

	public MedianPreReducer(int pos, TypeInformation<T> typeInfo, ExecutionConfig config) {
		this.fieldAccessor = FieldAccessor.create(pos, typeInfo, config);
		this.serializer = typeInfo.createSerializer(config);
	}

	public MedianPreReducer(String field, TypeInformation<T> typeInfo, ExecutionConfig config) {
		this.fieldAccessor = FieldAccessor.create(field, typeInfo, config);
		this.serializer = typeInfo.createSerializer(config);
	}

	private void updateMedian() {
		assert low.size() == high.size() || low.size() == high.size() + 1;
		if (low.size() == 0) {
			median = null;
		} else if (low.size() == high.size()) {
			// This is essentially  (low.last + high.first) / 2,  but we have to drill down to the double field
			median = serializer.copy(elements.getLast());
			median = fieldAccessor.set(median, (fieldAccessor.get(low.last()) + fieldAccessor.get(high.first())) / 2);
		} else {
			// low.last
			median = serializer.copy(elements.getLast());
			median = fieldAccessor.set(median, fieldAccessor.get(low.last()));
		}
	}

	private void moveUpIfNeccessary() {
		if (low.size() > high.size() + 1) {
			high.add(low.pollLast());
		}
	}
	private void moveDownIfNeccessary() {
		if (low.size() < high.size()) {
			low.add(high.pollFirst());
		}
	}

	@Override
	public void store(T elem) throws Exception {
		if (median == null) {
			low.add(elem);
		} else if(fieldAccessor.get(elem) <= fieldAccessor.get(median)) {
			low.add(elem);
			moveUpIfNeccessary();
		} else if (fieldAccessor.get(elem) > fieldAccessor.get(median)) {
			high.add(elem);
			moveDownIfNeccessary();
		}
		elements.addLast(elem);
		updateMedian();
	}

	@Override
	public void evict(int n) {
		for (int i = 0; i < n; i++) {
			T elem = elements.pollFirst();
			if(elem == null) {
				break;
			}
			if (low.contains(elem)) {
				low.removeOne(elem);
				moveDownIfNeccessary();
			} else if (high.contains(elem)) {
				high.removeOne(elem);
				moveUpIfNeccessary();
			} else {
				throw new RuntimeException("Internal error in MedianPreReducer");
			}
		}
		updateMedian();
	}

	@Override
	public void emitWindow(Collector<StreamWindow<T>> collector) {
		if (median != null) {
			StreamWindow<T> currentWindow = createEmptyWindow();
			currentWindow.add(median);
			collector.collect(currentWindow);
		} else if (emitEmpty) {
			collector.collect(createEmptyWindow());
		}
	}

	@Override
	public WindowBuffer<T> clone() {
		return new MedianPreReducer<T>(fieldAccessor, serializer);
	}

	private class CompareOnField implements Comparator<T>, Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public int compare(T a, T b) {
			try {
				return fieldAccessor.get(a).compareTo(fieldAccessor.get(b));
			} catch (ClassCastException e) {
				throw new RuntimeException("ClassCastException in MedianPreReducer: " + e.getMessage()
						+ "\nWindowedDataStream.median only supports Double fields!");
			}
		}
	}

	// This multiset class is implemented by storing the counts of the elements in a TreeMap.
	static class TreeMultiset<T> extends TreeMap<T, Integer> {

		int size = 0;

		@Override
		public int size() {
			return size;
		}

		TreeMultiset(Comparator<T> comparator) {
			super(comparator);
		}

		T first() {
			return firstKey();
		}

		T last() {
			return lastKey();
		}

		T pollFirst() {
			if (!isEmpty()) {
				size--;
			}
			Entry<T, Integer> first = firstEntry();
			if (first.getValue() > 1) {
				put(first.getKey(), first.getValue() - 1);
				return first.getKey();
			} else {
				return pollFirstEntry().getKey();
			}
		}

		T pollLast() {
			if (!isEmpty()) {
				size--;
			}
			Entry<T, Integer> last = lastEntry();
			if (last.getValue() > 1) {
				put(last.getKey(), last.getValue() - 1);
				return last.getKey();
			} else {
				return pollLastEntry().getKey();
			}
		}

		void add(T elem) {
			size++;
			Integer oldCount = get(elem);
			if (oldCount == null) {
				oldCount = 0;
			}
			put(elem, oldCount + 1);
		}

		Boolean contains(T elem) {
			return get(elem) != null;
		}

		void removeOne(T elem) {
			assert contains(elem);
			size--;
			Integer oldCount = get(elem);
			if (oldCount > 1) {
				put(elem, oldCount - 1);
			} else {
				remove(elem);
			}
		}
	}
}
