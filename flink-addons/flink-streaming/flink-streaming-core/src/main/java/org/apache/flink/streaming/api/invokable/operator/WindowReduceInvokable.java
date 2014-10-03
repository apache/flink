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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;

public class WindowReduceInvokable<OUT> extends BatchReduceInvokable<OUT> {
	private static final long serialVersionUID = 1L;
	protected long startTime;
	protected long nextRecordTime;
	protected TimeStamp<OUT> timestamp;
	protected StreamWindow window;

	public WindowReduceInvokable(ReduceFunction<OUT> reduceFunction, long windowSize,
			long slideInterval, TimeStamp<OUT> timestamp) {
		super(reduceFunction, windowSize, slideInterval);
		this.timestamp = timestamp;
		this.startTime = timestamp.getStartTime();
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		this.window = new StreamWindow();
		this.batch = this.window;
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
		public void reduceToBuffer(OUT nextValue) throws Exception {

			checkWindowEnd(timestamp.getTimestamp(nextValue));

			if (currentValue != null) {
				currentValue = reducer.reduce(serializer.copy(currentValue), serializer.copy(nextValue));
			} else {
				currentValue = nextValue;
			}
		}

		protected synchronized void checkWindowEnd(long timeStamp) {
			nextRecordTime = timeStamp;

			while (miniBatchEnd()) {
				addToBuffer();
				if (batchEnd()) {
					reduceBatch();
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

		@Override
		public boolean batchEnd() {
			if (minibatchCounter == numberOfBatches) {
				minibatchCounter -= batchPerSlide;
				return true;
			}
			return false;
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
