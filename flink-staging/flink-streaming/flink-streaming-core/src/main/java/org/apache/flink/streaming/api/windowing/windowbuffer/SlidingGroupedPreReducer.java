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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.StreamWindow;

/*
 * Grouped pre-reducer for sliding eviction policy
 * (the slide size is smaller than the window size).
 */
public abstract class SlidingGroupedPreReducer<T> extends SlidingPreReducer<T> {

	private static final long serialVersionUID = 1L;

	protected Map<Object, T> currentReducedMap = new HashMap<Object, T>();
	protected LinkedList<Map<Object, T>> reducedMap = new LinkedList<Map<Object, T>>();

	protected KeySelector<T, ?> key;

	public SlidingGroupedPreReducer(ReduceFunction<T> reducer, TypeSerializer<T> serializer,
			KeySelector<T, ?> key) {
		super(reducer, serializer);
		this.key = key;
	}

	public boolean addFinalAggregate(StreamWindow<T> currentWindow) throws Exception {
		Map<Object, T> finalReduce = null;

		if (!reducedMap.isEmpty()) {
			finalReduce = reducedMap.get(0);
			for (int i = 1; i < reducedMap.size(); i++) {
				finalReduce = reduceMaps(finalReduce, reducedMap.get(i));

			}
			if (currentReducedMap != null) {
				finalReduce = reduceMaps(finalReduce, currentReducedMap);
			}

		} else {
			finalReduce = currentReducedMap;
		}

		if (finalReduce != null) {
			currentWindow.addAll(finalReduce.values());
			return true;
		} else {
			return false;
		}

	}

	private Map<Object, T> reduceMaps(Map<Object, T> first, Map<Object, T> second) throws Exception {

		Map<Object, T> reduced = new HashMap<Object, T>();

		// Get the common keys in the maps
		Set<Object> interSection = new HashSet<Object>();
		Set<Object> diffFirst = new HashSet<Object>();
		Set<Object> diffSecond = new HashSet<Object>();

		for (Object key : first.keySet()) {
			if (second.containsKey(key)) {
				interSection.add(key);
			} else {
				diffFirst.add(key);
			}
		}

		for (Object key : second.keySet()) {
			if (!interSection.contains(key)) {
				diffSecond.add(key);
			}
		}

		// Reduce the common keys
		for (Object key : interSection) {
			reduced.put(
					key,
					reducer.reduce(serializer.copy(first.get(key)),
							serializer.copy(second.get(key))));
		}

		for (Object key : diffFirst) {
			reduced.put(key, first.get(key));
		}

		for (Object key : diffSecond) {
			reduced.put(key, second.get(key));
		}

		return reduced;
	}

	protected void updateCurrent(T element) throws Exception {
		if (currentReducedMap == null) {
			currentReducedMap = new HashMap<Object, T>();
			currentReducedMap.put(key.getKey(element), element);
		} else {
			Object nextKey = key.getKey(element);
			T last = currentReducedMap.get(nextKey);
			if (last == null) {
				currentReducedMap.put(nextKey, element);
			} else {
				currentReducedMap.put(nextKey, reducer.reduce(serializer.copy(last), element));
			}
		}
	}

	@Override
	protected void removeLastReduced() {
		reducedMap.removeFirst();
	}

	@Override
	protected void addCurrentToBuffer(T element) throws Exception {
		reducedMap.add(currentReducedMap);
	}

	@Override
	protected void resetCurrent() {
		currentReducedMap = null;
		elementsSinceLastPreAggregate = 0;
	}

	@Override
	protected boolean currentNotEmpty() {
		return currentReducedMap != null;
	}
}
