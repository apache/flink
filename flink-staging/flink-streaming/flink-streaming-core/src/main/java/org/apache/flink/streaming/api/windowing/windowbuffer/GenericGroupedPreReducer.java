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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * This class makes a grouped pre-reducer from a non-grouped one, by creating an instance for every group
 * that appears in the window.
 */
public class GenericGroupedPreReducer<T, PR extends GenericGroupablePreReducer<T>>
		extends WindowBuffer<T> implements PreAggregator, Serializable {

	private static final long serialVersionUID = 1L;

	private KeySelector<T, ?> keySelector;

	// This will be cloned when a new group is created.
	private PR protoPreReducer;

	// PreReducers for the individual groups
	private Map<Object, PR> groupPreReducers = new HashMap<Object, PR>();

	// Holds a reference for the groupPreReducer belonging to each element currently in the window.
	// (This is used at evict, so that we have to index into groupPreReducers only on store.)
	private Queue<PR> groupPreReducerPerElement = new ArrayDeque<PR>();

	public GenericGroupedPreReducer(PR protoPreReducer, KeySelector<T, ?> keySelector) {
		this.protoPreReducer = protoPreReducer;
		this.keySelector = keySelector;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void store(T elem) throws Exception {
		Object key = keySelector.getKey(elem);
		PR groupPreReducer = groupPreReducers.get(key);
		if(groupPreReducer == null) { // Group doesn't exist yet, create it.
			groupPreReducer = (PR)protoPreReducer.clone();
			groupPreReducers.put(key, groupPreReducer);
		}
		groupPreReducer.store(elem);
		groupPreReducerPerElement.add(groupPreReducer);
	}

	@Override
	public void evict(int n) throws Exception {
		for (int i = 0; i < n; i++) {
			PR groupPreReducer = groupPreReducerPerElement.poll();
			if(groupPreReducer == null) {
				break;
			}
			groupPreReducer.evict(1);
		}
	}

	@Override
	public void emitWindow(Collector<StreamWindow<T>> collector) {
		StreamWindow<T> currentWindow = createEmptyWindow();
		for(Iterator<PR> it = groupPreReducers.values().iterator(); it.hasNext(); ) {
			PR groupPreReducer = it.next();
			T groupAggregate = groupPreReducer.getAggregate();
			if(groupAggregate != null) {
				currentWindow.add(groupAggregate);
			} else {
				it.remove(); // Remove groups that don't contain elements, to not leak memory for long ago seen groups.
			}
		}
		if(!currentWindow.isEmpty() || emitEmpty) {
			collector.collect(currentWindow);
		}
	}

	@Override
	public WindowBuffer<T> clone() {
		return new GenericGroupedPreReducer<T, PR>(protoPreReducer, keySelector);
	}

}
