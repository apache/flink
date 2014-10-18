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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;

public class WindowGroupReduceInvokable<IN, OUT> extends BatchGroupReduceInvokable<IN, OUT> {

	private static final long serialVersionUID = 1L;
	private long startTime;
	protected long nextRecordTime;
	protected TimeStamp<IN> timestamp;
	protected StreamWindow window;

	public WindowGroupReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long windowSize,
			long slideInterval, TimeStamp<IN> timestamp) {
		super(reduceFunction, windowSize, slideInterval);
		this.timestamp = timestamp;
		this.startTime = timestamp.getStartTime();
		this.window = new StreamWindow();
		this.batch = this.window;
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		if (timestamp instanceof DefaultTimeStamp) {
			(new TimeCheck()).start();
		}
	}

	protected class StreamWindow extends StreamBatch {

		private static final long serialVersionUID = 1L;

		public StreamWindow() {
			super();
		}

		@Override
		public void addToBuffer(IN nextValue) throws Exception {
			checkWindowEnd(timestamp.getTimestamp(nextValue));
			if (minibatchCounter >= 0) {
				circularList.add(nextValue);
			}
		}

		protected synchronized void checkWindowEnd(long timeStamp) {
			nextRecordTime = timeStamp;

			while (miniBatchEnd()) {
				circularList.newSlide();
				minibatchCounter++;
				if (batchEnd()) {
					reduceBatch();
					circularList.shiftWindow(batchPerSlide);
				}
			}
		}

		@Override
		protected boolean miniBatchEnd() {
			if (nextRecordTime < startTime + granularity) {
				return false;
			} else {
				startTime += granularity;
				return true;
			}
		}

	}

	private class TimeCheck extends Thread {
		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(slideSize);
				} catch (InterruptedException e) {
				}
				if (isRunning) {
					window.checkWindowEnd(System.currentTimeMillis());
				} else {
					break;
				}
			}
		}
	}

}
