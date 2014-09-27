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

package org.apache.flink.streaming.api.invokable.operator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class GroupedWindowReduceInvokable<OUT> extends WindowReduceInvokable<OUT> {

	private static final long serialVersionUID = 1L;
	private int keyPosition;

	public GroupedWindowReduceInvokable(ReduceFunction<OUT> reduceFunction, long windowSize,
			long slideInterval, int keyPosition, TimeStamp<OUT> timestamp) {
		super(reduceFunction, windowSize, slideInterval, timestamp);
		this.keyPosition = keyPosition;
		this.window = new GroupedStreamWindow();
		this.batch = this.window;
	}
	
	@Override
	protected void callUserFunction() throws Exception {	
		@SuppressWarnings("unchecked")
		Iterator<Map<Object, OUT>> reducedIterator = (Iterator<Map<Object, OUT>>) batch.getIterator();
		Map<Object, OUT> reducedValues = reducedIterator.next();

		while (reducedIterator.hasNext()) {
			Map<Object, OUT> nextValues = reducedIterator.next();
			for (Entry<Object, OUT> entry : nextValues.entrySet()) {
				OUT currentValue = reducedValues.get(entry.getKey());
				if (currentValue == null) {
					reducedValues.put(entry.getKey(), entry.getValue());
				} else {
					reducedValues.put(entry.getKey(), reducer.reduce(currentValue, entry.getValue()));
				}
			}
		}
		for (OUT value : reducedValues.values()) {
			collector.collect(value);
		}
	}
	

	protected class GroupedStreamWindow extends StreamWindow {

		private static final long serialVersionUID = 1L;
		private Map<Object, OUT> currentValues;

		public GroupedStreamWindow() {
			super();
			this.currentValues  = new HashMap<Object, OUT>();
		}

		@Override
		public void reduceToBuffer(StreamRecord<OUT> next) throws Exception {

			OUT nextValue = next.getObject();
			Object key = next.getField(keyPosition);
			checkBatchEnd(timestamp.getTimestamp(nextValue));

			OUT currentValue = currentValues.get(key);
			if (currentValue != null) {
				currentValues.put(key, reducer.reduce(currentValue, nextValue));
			}else{
				currentValues.put(key, nextValue);
			}

		}
		
		@Override
		public boolean miniBatchInProgress() {
			return !currentValues.isEmpty();
		};

		@SuppressWarnings("unchecked")
		@Override
		protected void addToBuffer() {
			Map<Object, OUT> reuseMap;
			
			if (circularBuffer.isFull()) {
				reuseMap = (Map<Object, OUT>) circularBuffer.remove();
				reuseMap.clear();
			} else {
				reuseMap = new HashMap<Object, OUT>(currentValues.size());
			}
			
			circularBuffer.add(currentValues);
			minibatchCounter++;
			currentValues = reuseMap;
		}

	}

}
