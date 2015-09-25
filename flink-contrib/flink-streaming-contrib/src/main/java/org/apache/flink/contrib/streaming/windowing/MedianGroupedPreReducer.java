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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.windowbuffer.PreAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;
import org.apache.flink.streaming.util.FieldAccessor;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * Grouped pre-reducer for calculating median with any eviction policy.
 *
 * It stores a MedianPreReducer for every group.
 */
public class MedianGroupedPreReducer<T> extends WindowBuffer<T> implements PreAggregator, Serializable {

	private static final long serialVersionUID = 1L;

	// This is for getting and setting the field specified in the parameters to the median call.
	private FieldAccessor<T, Double> fieldAccessor;

	private TypeSerializer<T> serializer;

	private KeySelector<T, ?> keySelector;

	// PreReducers for the individual groups
	private Map<Object, MedianPreReducer<T>> groupPreReducers = new TreeMap<Object, MedianPreReducer<T>>();

	// Holds a reference for the groupPreReducer belonging to each element currently in the window.
	// (This is used at evict, so that we have to index into groupPreReducers only on store.)
	private Queue<MedianPreReducer<T>> groupPreReducerPerElement = new ArrayDeque<MedianPreReducer<T>>();

	public MedianGroupedPreReducer(FieldAccessor<T, Double> fieldAccessor, TypeSerializer<T> serializer,
									KeySelector<T, ?> keySelector) {
		this.fieldAccessor = fieldAccessor;
		this.serializer = serializer;
		this.keySelector = keySelector;
	}

	public MedianGroupedPreReducer(int pos, TypeInformation<T> typeInfo, ExecutionConfig config,
									KeySelector<T, ?> keySelector) {
		this.fieldAccessor = FieldAccessor.create(pos, typeInfo, config);
		this.serializer = typeInfo.createSerializer(config);
		this.keySelector = keySelector;
	}

	public MedianGroupedPreReducer(String field, TypeInformation<T> typeInfo, ExecutionConfig config,
									KeySelector<T, ?> keySelector) {
		this.fieldAccessor = FieldAccessor.create(field, typeInfo, config);
		this.serializer = typeInfo.createSerializer(config);
		this.keySelector = keySelector;
	}

	@Override
	public void store(T elem) throws Exception {
		Object key = keySelector.getKey(elem);
		MedianPreReducer<T> groupPreReducer = groupPreReducers.get(key);
		if (groupPreReducer == null) { // Group doesn't exist yet, create it.
			groupPreReducer = new MedianPreReducer<T>(fieldAccessor, serializer);
			groupPreReducers.put(key, groupPreReducer);
		}
		groupPreReducer.store(elem);
		groupPreReducerPerElement.add(groupPreReducer);
	}

	@Override
	public void evict(int n) {
		for (int i = 0; i < n; i++) {
			MedianPreReducer<T> groupPreReducer = groupPreReducerPerElement.poll();
			if (groupPreReducer == null) {
				break;
			}
			groupPreReducer.evict(1);
		}
	}

	@Override
	public void emitWindow(Collector<StreamWindow<T>> collector) {
		StreamWindow<T> currentWindow = createEmptyWindow();
		for(Iterator<MedianPreReducer<T>> it = groupPreReducers.values().iterator(); it.hasNext(); ) {
			MedianPreReducer<T> groupPreReducer = it.next();
			T groupMedian = groupPreReducer.getMedian();
			if (groupMedian != null) {
				currentWindow.add(groupMedian);
			} else {
				it.remove(); // Remove groups that don't contain elements, to not leak memory for long ago seen groups.
			}
		}
		if (!currentWindow.isEmpty() || emitEmpty) {
			collector.collect(currentWindow);
		}
	}

	@Override
	public WindowBuffer<T> clone() {
		return new MedianGroupedPreReducer<T>(fieldAccessor, serializer, keySelector);
	}

}
