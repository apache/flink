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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class CoWindowReduceInvokable<IN1, IN2, OUT> extends CoBatchReduceInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;
	protected long startTime1;
	protected long startTime2;
	protected long nextRecordTime1;
	protected long nextRecordTime2;
	protected TimeStamp<IN1> timestamp1;
	protected TimeStamp<IN2> timestamp2;

	public CoWindowReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer, long windowSize1,
			long windowSize2, long slideInterval1, long slideInterval2, TimeStamp<IN1> timestamp1,
			TimeStamp<IN2> timestamp2) {
		super(coReducer, windowSize1, windowSize2, slideInterval1, slideInterval2);
		this.timestamp1 = timestamp1;
		this.timestamp2 = timestamp2;
		this.startTime1 = timestamp1.getStartTime();
		this.startTime2 = timestamp2.getStartTime();

	}

	@Override
	public void reduceToBuffer1(StreamRecord<IN1> next, StreamBatch<IN1> streamWindow)
			throws Exception {
		IN1 nextValue = next.getObject();

		checkBatchEnd1(timestamp1.getTimestamp(nextValue), streamWindow);

		if (streamWindow.currentValue != null) {
			streamWindow.currentValue = coReducer.reduce1(
					serializer1.copy(streamWindow.currentValue), serializer1.copy(nextValue));
		} else {
			streamWindow.currentValue = nextValue;
		}
	}

	@Override
	public void reduceToBuffer2(StreamRecord<IN2> next, StreamBatch<IN2> streamWindow)
			throws Exception {
		IN2 nextValue = next.getObject();

		checkBatchEnd2(timestamp2.getTimestamp(nextValue), streamWindow);

		if (streamWindow.currentValue != null) {
			streamWindow.currentValue = coReducer.reduce2(
					serializer2.copy(streamWindow.currentValue), serializer2.copy(nextValue));
		} else {
			streamWindow.currentValue = nextValue;
		}
	}

	protected synchronized void checkBatchEnd1(long timeStamp, StreamBatch<IN1> streamWindow) {
		nextRecordTime1 = timeStamp;

		while (miniBatchEnd1()) {
			((StreamWindow<IN1>) streamWindow).addToBuffer();
			if (((StreamWindow<IN1>) streamWindow).batchEnd()) {
				reduceBatch1((StreamWindow<IN1>) streamWindow);
			}
		}
	}

	protected synchronized void checkBatchEnd2(long timeStamp, StreamBatch<IN2> streamWindow) {
		nextRecordTime2 = timeStamp;

		while (miniBatchEnd2()) {
			(streamWindow).addToBuffer();
			if (((StreamWindow<IN2>) streamWindow).batchEnd()) {
				reduceBatch2(streamWindow);
			}
		}
	}

	protected boolean miniBatchEnd1() {
		if (nextRecordTime1 < startTime1 + granularity1) {
			return false;
		} else {
			startTime1 += granularity1;
			return true;
		}
	}

	protected boolean miniBatchEnd2() {
		if (nextRecordTime2 < startTime2 + granularity2) {
			return false;
		} else {
			startTime2 += granularity2;
			return true;
		}
	}

	protected class StreamWindow<IN> extends StreamBatch<IN> {
		private static final long serialVersionUID = 1L;

		public StreamWindow(long windowSize, long slideInterval) {
			super(windowSize, slideInterval);
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

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		this.batch1 = new StreamWindow<IN1>(batchSize1, slideSize1);
		this.batch2 = new StreamWindow<IN2>(batchSize2, slideSize2);
		if (timestamp1 instanceof DefaultTimeStamp) {
			(new TimeCheck1()).start();
		}
		if (timestamp2 instanceof DefaultTimeStamp) {
			(new TimeCheck2()).start();
		}
	}

	private class TimeCheck1 extends Thread {
		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(slideSize1);
				} catch (InterruptedException e) {
				}
				if (isRunning) {
					checkBatchEnd1(System.currentTimeMillis(), (StreamWindow<IN1>) batch1);
				} else {
					break;
				}
			}
		}
	}

	private class TimeCheck2 extends Thread {
		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(slideSize2);
				} catch (InterruptedException e) {
				}
				if (isRunning) {
					checkBatchEnd2(System.currentTimeMillis(), (StreamWindow<IN2>) batch2);
				} else {
					break;
				}
			}
		}
	}

}
