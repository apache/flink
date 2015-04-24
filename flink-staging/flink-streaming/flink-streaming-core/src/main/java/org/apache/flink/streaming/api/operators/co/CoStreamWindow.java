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

package org.apache.flink.streaming.api.operators.co;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math.util.MathUtils;
import org.apache.flink.streaming.api.functions.co.CoWindowFunction;
import org.apache.flink.streaming.api.state.CircularFifoList;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class CoStreamWindow<IN1, IN2, OUT> extends CoStreamOperator<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	protected long windowSize;
	protected long slideSize;
	protected CircularFifoList<StreamRecord<IN1>> circularList1;
	protected CircularFifoList<StreamRecord<IN2>> circularList2;
	protected TimestampWrapper<IN1> timeStamp1;
	protected TimestampWrapper<IN2> timeStamp2;

	protected StreamWindow window;

	protected long startTime;
	protected long nextRecordTime;

	public CoStreamWindow(CoWindowFunction<IN1, IN2, OUT> coWindowFunction, long windowSize,
			long slideInterval, TimestampWrapper<IN1> timeStamp1, TimestampWrapper<IN2> timeStamp2) {
		super(coWindowFunction);
		this.windowSize = windowSize;
		this.slideSize = slideInterval;
		this.circularList1 = new CircularFifoList<StreamRecord<IN1>>();
		this.circularList2 = new CircularFifoList<StreamRecord<IN2>>();
		this.timeStamp1 = timeStamp1;
		this.timeStamp2 = timeStamp2;
		this.startTime = timeStamp1.getStartTime();

		this.window = new StreamWindow();
	}

	@Override
	protected void handleStream1() throws Exception {
		window.addToBuffer1(reuse1.getObject());
	}

	@Override
	protected void handleStream2() throws Exception {
		window.addToBuffer2(reuse2.getObject());
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void callUserFunction() throws Exception {

		List<IN1> first = new ArrayList<IN1>();
		List<IN2> second = new ArrayList<IN2>();

		for (IN1 element : window.circularList1.getElements()) {
			first.add(serializer1.copy(element));
		}
		for (IN2 element : window.circularList2.getElements()) {
			second.add(serializer2.copy(element));
		}

		if (!window.circularList1.isEmpty() || !window.circularList2.isEmpty()) {
			((CoWindowFunction<IN1, IN2, OUT>) userFunction).coWindow(first, second, collector);
		}
	}

	protected class StreamWindow implements Serializable {
		private static final long serialVersionUID = 1L;

		protected int granularity;
		protected int batchPerSlide;
		protected long numberOfBatches;

		protected long minibatchCounter;

		protected CircularFifoList<IN1> circularList1;
		protected CircularFifoList<IN2> circularList2;

		public StreamWindow() {
			this.granularity = (int) MathUtils.gcd(windowSize, slideSize);
			this.batchPerSlide = (int) (slideSize / granularity);
			this.numberOfBatches = windowSize / granularity;
			this.circularList1 = new CircularFifoList<IN1>();
			this.circularList2 = new CircularFifoList<IN2>();
			this.minibatchCounter = 0;
		}

		public void addToBuffer1(IN1 nextValue) throws Exception {
			checkWindowEnd(timeStamp1.getTimestamp(nextValue));
			if (minibatchCounter >= 0) {
				circularList1.add(nextValue);
			}
		}

		public void addToBuffer2(IN2 nextValue) throws Exception {
			checkWindowEnd(timeStamp2.getTimestamp(nextValue));
			if (minibatchCounter >= 0) {
				circularList2.add(nextValue);
			}
		}

		protected synchronized void checkWindowEnd(long timeStamp) {
			nextRecordTime = timeStamp;

			while (miniBatchEnd()) {
				circularList1.newSlide();
				circularList2.newSlide();
				minibatchCounter++;
				if (windowEnd()) {
					callUserFunctionAndLogException();
					circularList1.shiftWindow(batchPerSlide);
					circularList2.shiftWindow(batchPerSlide);
				}
			}
		}

		protected boolean miniBatchEnd() {
			if (nextRecordTime < startTime + granularity) {
				return false;
			} else {
				startTime += granularity;
				return true;
			}
		}

		public boolean windowEnd() {
			if (minibatchCounter == numberOfBatches) {
				minibatchCounter -= batchPerSlide;
				return true;
			}
			return false;
		}

		public void reduceLastBatch() {
			if (!miniBatchEnd()) {
				callUserFunctionAndLogException();
			}
		}

		public Iterable<IN1> getIterable1() {
			return circularList1.getIterable();
		}

		public Iterable<IN2> getIterable2() {
			return circularList2.getIterable();
		}

		@Override
		public String toString() {
			return circularList1.toString();
		}

	}

	@Override
	public void close() {
		if (!window.miniBatchEnd()) {
			callUserFunctionAndLogException();
		}
		super.close();
	}

	@Override
	protected void callUserFunction1() throws Exception {
	}

	@Override
	protected void callUserFunction2() throws Exception {
	}

	public void setSlideSize(long slideSize) {
		this.slideSize = slideSize;
	}

}
