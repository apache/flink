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
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.util.Collector;

/**
 * Non-grouped pre-reducer for tumbling eviction policy.
 */
public class TumblingPreReducer<T> implements WindowBuffer<T>, CompletePreAggregator {

	private static final long serialVersionUID = 1L;

	private ReduceFunction<T> reducer;

	private T reduced;
	private int numOfElements = 0;
	private TypeSerializer<T> serializer;

	public TumblingPreReducer(ReduceFunction<T> reducer, TypeSerializer<T> serializer) {
		this.reducer = reducer;
		this.serializer = serializer;
	}

	public boolean emitWindow(Collector<StreamWindow<T>> collector) {
		if (reduced != null) {
			StreamWindow<T> currentWindow = new StreamWindow<T>();
			currentWindow.add(reduced);
			collector.collect(currentWindow);
			reduced = null;
			numOfElements = 0;
			return true;
		} else {
			return false;
		}
	}

	public void store(T element) throws Exception {
		if (reduced == null) {
			reduced = element;
		} else {
			reduced = reducer.reduce(serializer.copy(reduced), element);
		}
		numOfElements++;
	}

	public void evict(int n) {
	}

	public int size() {
		return numOfElements;
	}

	@Override
	public TumblingPreReducer<T> clone() {
		return new TumblingPreReducer<T>(reducer, serializer);
	}

	@Override
	public String toString() {
		return reduced.toString();
	}

}
