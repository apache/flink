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

import org.apache.flink.api.common.functions.ReduceFunctionWithInverse;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Non-grouped pre-reducer for the situation when we know the inverse of the reduce function (see the javadoc of
 * reduceWindow). This works for any eviction and trigger policy.
 */
public class InversePreReducer<T> extends GenericGroupablePreReducer<T> {

	private static final long serialVersionUID = 1L;

	private ReduceFunctionWithInverse<T> reducer;

	private Queue<T> currentElements = new ArrayDeque<T>();

	private T reduced;
	private TypeSerializer<T> serializer;

	@Override
	public T getAggregate() {
		return reduced;
	}

	public InversePreReducer(ReduceFunctionWithInverse<T> reducer, TypeSerializer<T> serializer) {
		this.reducer = reducer;
		this.serializer = serializer;
	}

	public void emitWindow(Collector<StreamWindow<T>> collector) {
		if (reduced != null) {
			StreamWindow<T> currentWindow = createEmptyWindow();
			currentWindow.add(reduced);
			collector.collect(currentWindow);
		} else if (emitEmpty) {
			collector.collect(createEmptyWindow());
		}
	}

	public void store(T elem) throws Exception {
		currentElements.add(serializer.copy(elem));
		if (reduced == null) {
			reduced = serializer.copy(elem);
		} else {
			reduced = reducer.reduce(reduced, serializer.copy(elem));
		}
	}

	public void evict(int n) throws Exception {
		for(int i = 0; i < n; i++) {
			T elem = currentElements.poll();
			if(elem == null) {
				break;
			}
			if (reduced != null) {
				reduced = reducer.invReduce(reduced, elem);
			}
		}
		if(currentElements.size() == 0) {
			reduced = null;
		}
	}

	@Override
	public InversePreReducer<T> clone() {
		return new InversePreReducer<T>(reducer, serializer);
	}

	@Override
	public String toString() {
		return reduced.toString();
	}

}
