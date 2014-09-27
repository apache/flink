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

import java.util.Iterator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.CircularFifoList;
import org.apache.flink.streaming.state.StreamIterator;

public abstract class CoGroupReduceInvokable<IN1, IN2, OUT> extends CoInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	protected CoGroupReduceFunction<IN1, IN2, OUT> coReducer;
	protected StreamIterator<IN1> userIterator1;
	protected StreamIterator<IN2> userIterator2;
	protected Iterable<IN1> userIterable1;
	protected Iterable<IN2> userIterable2;
	protected long windowSize1;
	protected long windowSize2;
	protected long slideInterval1;
	protected long slideInterval2;
	protected CircularFifoList<StreamRecord<IN1>> circularList1;
	protected CircularFifoList<StreamRecord<IN2>> circularList2;
	protected long WindowStartTime1;
	protected long WindowStartTime2;
	protected long WindowEndTime1;
	protected long WindowEndTime2;

	public CoGroupReduceInvokable(CoGroupReduceFunction<IN1, IN2, OUT> reduceFunction,
			long windowSize1, long windowSize2, long slideInterval1, long slideInterval2) {
		super(reduceFunction);
		this.coReducer = reduceFunction;
		this.userIterator1 = new StreamIterator<IN1>();
		this.userIterator2 = new StreamIterator<IN2>();
		this.windowSize1 = windowSize1;
		this.windowSize2 = windowSize2;
		this.slideInterval1 = slideInterval1;
		this.slideInterval2 = slideInterval2;
		this.circularList1 = new CircularFifoList<StreamRecord<IN1>>();
		this.circularList2 = new CircularFifoList<StreamRecord<IN2>>();
	}

	@Override
	protected void mutableInvoke() throws Exception {
		throw new RuntimeException("Reducing mutable sliding batch is not supported.");
	}

	@Override
	protected void handleStream1() throws Exception {
		while (windowStart1()) {
			circularList1.newSlide();
		}
		while (windowEnd1()) {
			reduce1();
			circularList1.shiftWindow();
		}
		circularList1.add(reuse1);
	}

	@Override
	protected void handleStream2() throws Exception {
		while (windowStart2()) {
			circularList2.newSlide();
		}
		while (windowEnd2()) {
			reduce2();
			circularList2.shiftWindow();
		}
		circularList2.add(reuse2);
	}

	protected void reduce1() throws Exception {
		userIterator1.load(circularList1.getIterator());
		callUserFunctionAndLogException1();
	}

	protected void reduce2() throws Exception {
		userIterator2.load(circularList2.getIterator());
		callUserFunctionAndLogException2();
	}

	@Override
	protected void callUserFunction1() throws Exception {
		coReducer.reduce1(userIterable1, collector);
	}

	@Override
	protected void callUserFunction2() throws Exception {
		coReducer.reduce2(userIterable2, collector);
	}

	protected abstract boolean windowStart1() throws Exception;

	protected abstract boolean windowStart2() throws Exception;

	protected abstract boolean windowEnd1() throws Exception;

	protected abstract boolean windowEnd2() throws Exception;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		userIterable1 = new BatchIterable1();
		userIterable2 = new BatchIterable2();
	}

	protected class BatchIterable1 implements Iterable<IN1> {
		@Override
		public Iterator<IN1> iterator() {
			return userIterator1;
		}
	}

	protected class BatchIterable2 implements Iterable<IN2> {
		@Override
		public Iterator<IN2> iterator() {
			return userIterator2;
		}
	}

}
