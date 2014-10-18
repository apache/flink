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
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class GroupedWindowReduceInvokable<OUT> extends WindowReduceInvokable<OUT> {

	private static final long serialVersionUID = 1L;
	private int keyPosition;
	private Map<Object, StreamWindow> streamWindows;
	private long currentMiniBatchCount = 0;

	public GroupedWindowReduceInvokable(ReduceFunction<OUT> reduceFunction, long windowSize,
			long slideInterval, int keyPosition, TimeStamp<OUT> timestamp) {
		super(reduceFunction, windowSize, slideInterval, timestamp);
		this.keyPosition = keyPosition;
		this.streamWindows = new HashMap<Object, StreamWindow>();
	}

	@Override
	protected StreamBatch getBatch(StreamRecord<OUT> next) {
		Object key = next.getField(keyPosition);
		StreamWindow window = streamWindows.get(key);
		if (window == null) {
			window = new GroupedStreamWindow();
			window.minibatchCounter = currentMiniBatchCount;
			streamWindows.put(key, window);
		}
		this.window = window;
		return window;
	}

	private void addToAllBuffers() {
		for (StreamBatch window : streamWindows.values()) {
			window.addToBuffer();
		}
	}

	private void reduceAllWindows() {
		for (StreamBatch window : streamWindows.values()) {
			window.minibatchCounter -= batchPerSlide;
			window.reduceBatch();
		}
	}

	@Override
	protected void reduceLastBatch() throws Exception {
		for (StreamBatch window : streamWindows.values()) {
			window.reduceLastBatch();
		}
	}

	protected class GroupedStreamWindow extends StreamWindow {

		private static final long serialVersionUID = 1L;

		public GroupedStreamWindow() {
			super();
		}

		@Override
		protected synchronized void checkWindowEnd(long timeStamp) {
			nextRecordTime = timeStamp;

			while (miniBatchEnd()) {
				addToAllBuffers();
				if (batchEnd()) {
					reduceAllWindows();
				}
			}
			currentMiniBatchCount = this.minibatchCounter;
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
