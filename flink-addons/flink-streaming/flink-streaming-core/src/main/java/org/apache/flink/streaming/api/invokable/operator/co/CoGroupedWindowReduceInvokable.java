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

package org.apache.flink.streaming.api.invokable.operator.co;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class CoGroupedWindowReduceInvokable<IN1, IN2, OUT> extends
		CoWindowReduceInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;
	private int keyPosition1;
	private int keyPosition2;

	public CoGroupedWindowReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer,
			long windowSize1, long windowSize2, long slideInterval1, long slideInterval2,
			int keyPosition1, int keyPosition2, TimeStamp<IN1> timestamp1, TimeStamp<IN2> timestamp2) {
		super(coReducer, windowSize1, windowSize2, slideInterval1, slideInterval2, timestamp1,
				timestamp2);
		this.keyPosition1 = keyPosition1;
	}

	@Override
	protected void callUserFunction1() throws Exception {
		@SuppressWarnings("unchecked")
		Iterator<Map<Object, IN1>> reducedIterator = (Iterator<Map<Object, IN1>>) batch1
				.getIterator();
		Map<Object, IN1> reducedValues = reducedIterator.next();

		while (reducedIterator.hasNext()) {
			Map<Object, IN1> nextValues = reducedIterator.next();
			for (Entry<Object, IN1> entry : nextValues.entrySet()) {
				IN1 currentValue = reducedValues.get(entry.getKey());
				if (currentValue == null) {
					reducedValues.put(entry.getKey(), entry.getValue());
				} else {
					reducedValues.put(entry.getKey(),
							coReducer.reduce1(currentValue, entry.getValue()));
				}
			}
		}
		for (IN1 value : reducedValues.values()) {
			collector.collect(coReducer.map1(value));
		}
	}

	@Override
	protected void callUserFunction2() throws Exception {
		@SuppressWarnings("unchecked")
		Iterator<Map<Object, IN2>> reducedIterator = (Iterator<Map<Object, IN2>>) batch2
				.getIterator();
		Map<Object, IN2> reducedValues = reducedIterator.next();

		while (reducedIterator.hasNext()) {
			Map<Object, IN2> nextValues = reducedIterator.next();
			for (Entry<Object, IN2> entry : nextValues.entrySet()) {
				IN2 currentValue = reducedValues.get(entry.getKey());
				if (currentValue == null) {
					reducedValues.put(entry.getKey(), entry.getValue());
				} else {
					reducedValues.put(entry.getKey(),
							coReducer.reduce2(currentValue, entry.getValue()));
				}
			}
		}
		for (IN2 value : reducedValues.values()) {
			collector.collect(coReducer.map2(value));
		}
	}

	@Override
	public void reduceToBuffer1(StreamRecord<IN1> next, StreamBatch<IN1> streamBatch)
			throws Exception {

		IN1 nextValue = next.getObject();
		Object key = next.getField(keyPosition1);
		checkBatchEnd1(timestamp1.getTimestamp(nextValue), streamBatch);

		IN1 currentValue = ((GroupedStreamWindow<IN1>) streamBatch).currentValues.get(key);
		if (currentValue != null) {
			((GroupedStreamWindow<IN1>) streamBatch).currentValues.put(key,
					coReducer.reduce1(currentValue, nextValue));
		} else {
			((GroupedStreamWindow<IN1>) streamBatch).currentValues.put(key, nextValue);
		}

	}

	@Override
	public void reduceToBuffer2(StreamRecord<IN2> next, StreamBatch<IN2> streamBatch)
			throws Exception {

		IN2 nextValue = next.getObject();
		Object key = next.getField(keyPosition2);
		checkBatchEnd2(timestamp2.getTimestamp(nextValue), streamBatch);

		IN2 currentValue = ((GroupedStreamWindow<IN2>) streamBatch).currentValues.get(key);
		if (currentValue != null) {
			((GroupedStreamWindow<IN2>) streamBatch).currentValues.put(key,
					coReducer.reduce2(currentValue, nextValue));
		} else {
			((GroupedStreamWindow<IN2>) streamBatch).currentValues.put(key, nextValue);
		}

	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		this.batch1 = new GroupedStreamWindow<IN1>(batchSize1, slideSize1);
		this.batch2 = new GroupedStreamWindow<IN2>(batchSize2, slideSize2);
	}

	protected class GroupedStreamWindow<IN> extends StreamWindow<IN> {

		private static final long serialVersionUID = 1L;
		private Map<Object, IN> currentValues;

		public GroupedStreamWindow(long windowSize, long slideInterval) {
			super(windowSize, slideInterval);
			this.currentValues = new HashMap<Object, IN>();
		}

		@Override
		public boolean miniBatchInProgress() {
			return !currentValues.isEmpty();
		};

		@SuppressWarnings("unchecked")
		@Override
		protected void addToBuffer() {
			Map<Object, IN> reuseMap;

			if (circularBuffer.isFull()) {
				reuseMap = (Map<Object, IN>) circularBuffer.remove();
				reuseMap.clear();
			} else {
				reuseMap = new HashMap<Object, IN>(currentValues.size());
			}

			circularBuffer.add(currentValues);
			changed = true;
			minibatchCounter++;
			currentValues = reuseMap;
		}

	}
}
