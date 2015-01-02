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
import java.util.Map;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

public class CoGroupedWindowReduceInvokable<IN1, IN2, OUT> extends
		CoWindowReduceInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;
	protected KeySelector<IN1, ?> keySelector1;
	protected KeySelector<IN2, ?> keySelector2;
	private Map<Object, StreamWindow<IN1>> streamWindows1;
	private Map<Object, StreamWindow<IN2>> streamWindows2;
	private long currentMiniBatchCount1 = 0;
	private long currentMiniBatchCount2 = 0;

	public CoGroupedWindowReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer,
			long windowSize1, long windowSize2, long slideInterval1, long slideInterval2,
			KeySelector<IN1, ?> keySelector1, KeySelector<IN2, ?> keySelector2,
			TimestampWrapper<IN1> timestamp1, TimestampWrapper<IN2> timestamp2) {
		super(coReducer, windowSize1, windowSize2, slideInterval1, slideInterval2, timestamp1,
				timestamp2);
		this.keySelector1 = keySelector1;
		this.keySelector2 = keySelector2;
		this.streamWindows1 = new HashMap<Object, StreamWindow<IN1>>();
		this.streamWindows2 = new HashMap<Object, StreamWindow<IN2>>();
	}

	@Override
	protected StreamBatch<IN1> getBatch1(StreamRecord<IN1> next) {
		Object key = next.getKey(keySelector1);
		StreamWindow<IN1> window = streamWindows1.get(key);
		if (window == null) {
			window = new GroupedStreamWindow<IN1>(batchSize1, slideSize1);
			window.minibatchCounter = currentMiniBatchCount1;
			streamWindows1.put(key, window);
		}
		this.window1 = window;
		return window;
	}

	@Override
	protected StreamBatch<IN2> getBatch2(StreamRecord<IN2> next) {
		Object key = next.getKey(keySelector2);
		StreamWindow<IN2> window = streamWindows2.get(key);
		if (window == null) {
			window = new GroupedStreamWindow<IN2>(batchSize2, slideSize2);
			window.minibatchCounter = currentMiniBatchCount2;
			streamWindows2.put(key, window);
		}
		this.window2 = window;
		return window;
	}

	private void addToAllBuffers1() {
		for (StreamBatch<IN1> window : streamWindows1.values()) {
			window.addToBuffer();
		}
	}

	private void addToAllBuffers2() {
		for (StreamBatch<IN2> window : streamWindows2.values()) {
			window.addToBuffer();
		}
	}

	private void reduceAllWindows1() {
		for (StreamBatch<IN1> window : streamWindows1.values()) {
			window.minibatchCounter -= batchPerSlide1;
			reduceBatch1((StreamBatch<IN1>) window);
		}
	}

	private void reduceAllWindows2() {
		for (StreamBatch<IN2> window : streamWindows2.values()) {
			window.minibatchCounter -= batchPerSlide2;
			reduceBatch2((StreamBatch<IN2>) window);
		}
	}

	@Override
	protected void reduceLastBatch1() throws Exception {
		for (StreamBatch<IN1> window : streamWindows1.values()) {
			reduceLastBatch1((StreamBatch<IN1>) window);
		}
	}

	@Override
	protected void reduceLastBatch2() throws Exception {
		for (StreamBatch<IN2> window : streamWindows2.values()) {
			reduceLastBatch2((StreamBatch<IN2>) window);
		}
	}

	@Override
	protected synchronized void checkWindowEnd1(long timeStamp, StreamWindow<IN1> streamWindow) {
		nextRecordTime1 = timeStamp;

		while (miniBatchEnd1()) {
			addToAllBuffers1();
			if (streamWindow.batchEnd()) {
				reduceAllWindows1();
			}
		}
		currentMiniBatchCount1 = streamWindow.minibatchCounter;
	}

	@Override
	protected synchronized void checkWindowEnd2(long timeStamp, StreamWindow<IN2> streamWindow) {
		nextRecordTime2 = timeStamp;

		while (miniBatchEnd2()) {
			addToAllBuffers2();
			if (streamWindow.batchEnd()) {
				reduceAllWindows2();
			}
		}
		currentMiniBatchCount2 = streamWindow.minibatchCounter;
	}

	protected class GroupedStreamWindow<IN> extends StreamWindow<IN> {
		private static final long serialVersionUID = 1L;

		public GroupedStreamWindow(long windowSize, long slideInterval) {
			super(windowSize, slideInterval);
		}

		@Override
		public boolean batchEnd() {
			if (minibatchCounter == numberOfBatches) {
				return true;
			}
			return false;
		}

	}
}
