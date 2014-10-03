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
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class GroupedWindowGroupReduceInvokable<IN, OUT> extends WindowGroupReduceInvokable<IN, OUT> {

	private static final long serialVersionUID = 1L;

	int keyPosition;
	Map<Object, StreamWindow> streamWindows;
	List<Object> cleanList;
	long currentMiniBatchCount = 0;

	public GroupedWindowGroupReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction,
			long windowSize, long slideInterval, int keyPosition, TimeStamp<IN> timestamp) {
		super(reduceFunction, windowSize, slideInterval, timestamp);
		this.keyPosition = keyPosition;
		this.reducer = reduceFunction;
		this.streamWindows = new HashMap<Object, StreamWindow>();
	}

	@Override
	protected StreamBatch getBatch(StreamRecord<IN> next) {
		Object key = next.getField(keyPosition);
		StreamWindow window = streamWindows.get(key);
		if (window == null) {
			window = new GroupedStreamWindow();
			for (int i = 0; i < currentMiniBatchCount; i++) {
				window.circularList.newSlide();
			}
			streamWindows.put(key, window);
		}
		this.window = window;
		return window;
	}

	@Override
	protected void reduceLastBatch() {
		for (StreamBatch window : streamWindows.values()) {
			window.reduceLastBatch();
		}
	}

	private void shiftGranularityAllWindows() {
		for (StreamBatch window : streamWindows.values()) {
			window.circularList.newSlide();
		}
	}

	private void slideAllWindows() {
		currentMiniBatchCount -= batchPerSlide;
		for (StreamBatch window : streamWindows.values()) {
			window.circularList.shiftWindow(batchPerSlide);
		}
	}

	private void reduceAllWindows() {
		for (StreamBatch window : streamWindows.values()) {
			window.reduceBatch();
		}
	}

	protected class GroupedStreamWindow extends StreamWindow {

		private static final long serialVersionUID = 1L;

		public GroupedStreamWindow() {
			super();
		}

		@Override
		public void addToBuffer(IN nextValue) throws Exception {
			checkWindowEnd(timestamp.getTimestamp(nextValue));
			if (currentMiniBatchCount >= 0) {
				circularList.add(nextValue);
			}
		}

		@Override
		protected synchronized void checkWindowEnd(long timeStamp) {
			nextRecordTime = timeStamp;

			while (miniBatchEnd()) {
				shiftGranularityAllWindows();
				currentMiniBatchCount += 1;
				if (batchEnd()) {
					reduceAllWindows();
					slideAllWindows();
				}
			}
		}

		@Override
		public boolean batchEnd() {
			if (currentMiniBatchCount == numberOfBatches) {
				return true;
			}
			return false;
		}

	}

}
