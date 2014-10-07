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

import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.math.util.MathUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.NullableCircularBuffer;

public class CoBatchReduceInvokable<IN1, IN2, OUT> extends CoInvokable<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;
	protected CoReduceFunction<IN1, IN2, OUT> coReducer;

	protected long slideSize1;
	protected long slideSize2;
	protected long batchSize1;
	protected long batchSize2;
	protected int granularity1;
	protected int granularity2;
	protected long batchPerSlide1;
	protected long batchPerSlide2;
	protected long numberOfBatches1;
	protected long numberOfBatches2;
	protected StreamBatch<IN1> batch1;
	protected StreamBatch<IN2> batch2;
	protected StreamBatch<IN1> currentBatch1;
	protected StreamBatch<IN2> currentBatch2;

	public CoBatchReduceInvokable(CoReduceFunction<IN1, IN2, OUT> coReducer, long batchSize1,
			long batchSize2, long slideSize1, long slideSize2) {
		super(coReducer);
		this.coReducer = coReducer;
		this.batchSize1 = batchSize1;
		this.batchSize2 = batchSize2;
		this.slideSize1 = slideSize1;
		this.slideSize2 = slideSize2;
		this.granularity1 = (int) MathUtils.gcd(batchSize1, slideSize1);
		this.granularity2 = (int) MathUtils.gcd(batchSize2, slideSize2);
		this.batchPerSlide1 = slideSize1 / granularity1;
		this.batchPerSlide2 = slideSize2 / granularity2;
		this.numberOfBatches1 = batchSize1 / granularity1;
		this.numberOfBatches2 = batchSize2 / granularity2;
	}

	@Override
	public void immutableInvoke() throws Exception {
		while (true) {
			int next = recordIterator.next(reuse1, reuse2);
			if (next == 0) {
				reduceLastBatch1();
				reduceLastBatch2();
				break;
			} else if (next == 1) {
				handleStream1();
				resetReuse1();
			} else {
				handleStream2();
				resetReuse2();
			}
		}
	}

	@Override
	protected void handleStream1() throws Exception {
		StreamBatch<IN1> batch1 = getBatch1(reuse1);
		reduceToBuffer1(reuse1, batch1);
	}

	@Override
	protected void handleStream2() throws Exception {
		StreamBatch<IN2> batch2 = getBatch2(reuse2);
		reduceToBuffer2(reuse2, batch2);
	}

	protected StreamBatch<IN1> getBatch1(StreamRecord<IN1> next) {
		return batch1;
	}

	protected StreamBatch<IN2> getBatch2(StreamRecord<IN2> next) {
		return batch2;
	}

	@Override
	// TODO: implement mutableInvoke for reduce
	protected void mutableInvoke() throws Exception {
		System.out.println("Immutable setting is used");
		immutableInvoke();
	}

	protected void reduce1(StreamBatch<IN1> batch) {
		this.currentBatch1 = batch;
		callUserFunctionAndLogException1();
	}

	protected void reduce2(StreamBatch<IN2> batch) {
		this.currentBatch2 = batch;
		callUserFunctionAndLogException2();
	}

	protected void reduceLastBatch1() throws Exception {
		reduceLastBatch1(batch1);
	}

	protected void reduceLastBatch2() throws Exception {
		reduceLastBatch2(batch2);
	}

	@Override
	protected void callUserFunction1() throws Exception {
		Iterator<IN1> reducedIterator = currentBatch1.getIterator();
		IN1 reduced = null;

		while (reducedIterator.hasNext() && reduced == null) {
			reduced = reducedIterator.next();
		}

		while (reducedIterator.hasNext()) {
			IN1 next = reducedIterator.next();
			if (next != null) {
				reduced = coReducer.reduce1(serializer1.copy(reduced), serializer1.copy(next));
			}
		}
		if (reduced != null) {
			collector.collect(coReducer.map1(serializer1.copy(reduced)));
		}
	}

	@Override
	protected void callUserFunction2() throws Exception {
		Iterator<IN2> reducedIterator = currentBatch2.getIterator();
		IN2 reduced = null;

		while (reducedIterator.hasNext() && reduced == null) {
			reduced = reducedIterator.next();
		}

		while (reducedIterator.hasNext()) {
			IN2 next = reducedIterator.next();
			if (next != null) {
				reduced = coReducer.reduce2(serializer2.copy(reduced), serializer2.copy(next));
			}
		}
		if (reduced != null) {
			collector.collect(coReducer.map2(serializer2.copy(reduced)));
		}
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		this.batch1 = new StreamBatch<IN1>(batchSize1, slideSize1);
		this.batch2 = new StreamBatch<IN2>(batchSize2, slideSize2);
	}

	public void reduceToBuffer1(StreamRecord<IN1> next, StreamBatch<IN1> streamBatch)
			throws Exception {
		IN1 nextValue = next.getObject();
		if (streamBatch.currentValue != null) {
			streamBatch.currentValue = coReducer.reduce1(
					serializer1.copy(streamBatch.currentValue), serializer1.copy(nextValue));
		} else {
			streamBatch.currentValue = nextValue;
		}

		streamBatch.counter++;

		if (streamBatch.miniBatchEnd()) {
			streamBatch.addToBuffer();
			if (streamBatch.batchEnd()) {
				reduceBatch1(streamBatch);
			}
		}
	}

	public void reduceToBuffer2(StreamRecord<IN2> next, StreamBatch<IN2> streamBatch)
			throws Exception {
		IN2 nextValue = next.getObject();
		if (streamBatch.currentValue != null) {
			streamBatch.currentValue = coReducer.reduce2(
					serializer2.copy(streamBatch.currentValue), serializer2.copy(nextValue));
		} else {
			streamBatch.currentValue = nextValue;
		}

		streamBatch.counter++;

		if (streamBatch.miniBatchEnd()) {
			streamBatch.addToBuffer();
			if (streamBatch.batchEnd()) {
				reduceBatch2(streamBatch);
			}
		}
	}

	public void reduceLastBatch1(StreamBatch<IN1> streamBatch) throws Exception {
		if (streamBatch.miniBatchInProgress()) {
			streamBatch.addToBuffer();
		}

		if (streamBatch.changed == true && streamBatch.minibatchCounter >= 0) {
			if (streamBatch.circularBuffer.isFull()) {
				for (long i = 0; i < (numberOfBatches1 - streamBatch.minibatchCounter); i++) {
					if (!streamBatch.circularBuffer.isEmpty()) {
						streamBatch.circularBuffer.remove();
					}
				}
			}
			if (!streamBatch.circularBuffer.isEmpty()) {
				reduce1(streamBatch);
			}
		}

	}

	public void reduceLastBatch2(StreamBatch<IN2> streamBatch) throws Exception {
		if (streamBatch.miniBatchInProgress()) {
			streamBatch.addToBuffer();
		}

		if (streamBatch.changed == true && streamBatch.minibatchCounter >= 0) {
			for (long i = 0; i < (numberOfBatches2 - streamBatch.minibatchCounter); i++) {
				if (!streamBatch.circularBuffer.isEmpty()) {
					streamBatch.circularBuffer.remove();
				}
			}
			if (!streamBatch.circularBuffer.isEmpty()) {
				reduce2(streamBatch);
			}
		}

	}

	public void reduceBatch1(StreamBatch<IN1> streamBatch) {
		reduce1(streamBatch);
		streamBatch.changed = false;
	}

	public void reduceBatch2(StreamBatch<IN2> streamBatch) {
		reduce2(streamBatch);
		streamBatch.changed = false;
	}

	protected class StreamBatch<IN> implements Serializable {
		private static final long serialVersionUID = 1L;

		protected long counter;
		protected long minibatchCounter;
		protected IN currentValue;
		protected long batchSize;
		protected long slideSize;
		protected long granularity;
		protected long batchPerSlide;
		protected long numberOfBatches;
		boolean changed;

		protected NullableCircularBuffer circularBuffer;

		public StreamBatch(long batchSize, long slideSize) {
			this.batchSize = batchSize;
			this.slideSize = slideSize;
			this.granularity = (int) MathUtils.gcd(batchSize, slideSize);
			this.batchPerSlide = slideSize / granularity;
			this.circularBuffer = new NullableCircularBuffer((int) (batchSize / granularity));
			this.counter = 0;
			this.minibatchCounter = 0;
			this.currentValue = null;
			this.numberOfBatches = batchSize / granularity;
			this.changed = false;

		}

		protected void addToBuffer() {
			circularBuffer.add(currentValue);
			changed = true;
			minibatchCounter++;
			currentValue = null;
		}

		protected boolean miniBatchEnd() {
			return (counter % granularity) == 0;
		}

		public boolean batchEnd() {
			if (counter == batchSize) {
				counter -= slideSize;
				minibatchCounter -= batchPerSlide;
				return true;
			}
			return false;
		}

		public boolean miniBatchInProgress() {
			return currentValue != null;
		}

		@SuppressWarnings("unchecked")
		public Iterator<IN> getIterator() {
			return circularBuffer.iterator();
		}

	}

}
