/**
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

package org.apache.flink.streaming.api.invokable.operator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.state.SlidingWindowState;

public class GroupedBatchReduceInvokable<OUT> extends BatchReduceInvokable<OUT> {

	private static final long serialVersionUID = 1L;
	protected transient SlidingWindowState<Map<Object, OUT>> intermediateValues;

	private int keyPosition;

	public GroupedBatchReduceInvokable(ReduceFunction<OUT> reduceFunction, long batchSize,
			long slideSize, int keyPosition) {
		super(reduceFunction, batchSize, slideSize);
		this.keyPosition = keyPosition;
	}

	protected void collectOneUnit() throws Exception {
		Map<Object, OUT> values = new HashMap<Object, OUT>();
		if (batchNotFull()) {
			do {
				Object key = reuse.getField(keyPosition);
				OUT nextValue = reuse.getObject();
				OUT currentValue = values.get(key);
				if (currentValue == null) {
					values.put(key, nextValue);
				} else {
					values.put(key, reducer.reduce(currentValue, nextValue));
				}
				resetReuse();
			} while (getNextRecord() != null && batchNotFull());
		}
		intermediateValues.pushBack(values);
	}

	@Override
	protected boolean isStateFull() {
		return intermediateValues.isFull();
	}

	@Override
	protected void callUserFunction() throws Exception {
		Iterator<Map<Object, OUT>> reducedIterator = intermediateValues.getBufferIterator();
		Map<Object, OUT> reducedValues = reducedIterator.next();

		while (reducedIterator.hasNext()) {
			Map<Object, OUT> nextValues = reducedIterator.next();
			for (Entry<Object, OUT> entry : nextValues.entrySet()) {
				OUT currentValue = reducedValues.get(entry.getKey());
				if (currentValue == null) {
					reducedValues.put(entry.getKey(), entry.getValue());
				} else {
					OUT next = typeSerializer.copy(entry.getValue(), reduceReuse);
					reducedValues.put(entry.getKey(), reducer.reduce(currentValue, next));
				}
			}
		}
		for (OUT value : reducedValues.values()) {
			collector.collect(value);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.intermediateValues = new SlidingWindowState<Map<Object, OUT>>(batchSize, slideSize,
				granularity);
	}

}