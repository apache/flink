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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.util.Collector;

/**
 * Grouped pre-reducer for tumbling eviction polciy.
 */
public class TumblingGroupedPreReducer<T> implements WindowBuffer<T>, CompletePreAggregator {

	private static final long serialVersionUID = 1L;

	private ReduceFunction<T> reducer;
	private KeySelector<T, ?> keySelector;

	private Map<Object, T> reducedValues;

	private int numOfElements = 0;
	private TypeSerializer<T> serializer;

	public TumblingGroupedPreReducer(ReduceFunction<T> reducer, KeySelector<T, ?> keySelector,
			TypeSerializer<T> serializer) {
		this.reducer = reducer;
		this.serializer = serializer;
		this.keySelector = keySelector;
		this.reducedValues = new HashMap<Object, T>();
	}

	public boolean emitWindow(Collector<StreamWindow<T>> collector) {

		if (!reducedValues.isEmpty()) {
			StreamWindow<T> currentWindow = new StreamWindow<T>();
			currentWindow.addAll(reducedValues.values());
			collector.collect(currentWindow);
			reducedValues.clear();
			numOfElements = 0;
			return true;
		} else {
			return false;
		}

	}

	public void store(T element) throws Exception {
		Object key = keySelector.getKey(element);

		T reduced = reducedValues.get(key);

		if (reduced == null) {
			reduced = element;
		} else {
			reduced = reducer.reduce(serializer.copy(reduced), element);
		}

		reducedValues.put(key, reduced);
		numOfElements++;
	}

	public void evict(int n) {
	}

	public int size() {
		return numOfElements;
	}

	@Override
	public TumblingGroupedPreReducer<T> clone() {
		return new TumblingGroupedPreReducer<T>(reducer, keySelector, serializer);
	}

	@Override
	public String toString() {
		return reducedValues.toString();
	}

}
