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

import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.math.util.MathUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.NullableCircularBuffer;

public class BatchReduceInvokable<OUT> extends StreamInvokable<OUT, OUT> {

	private static final long serialVersionUID = 1L;
	protected ReduceFunction<OUT> reducer;

	protected long slideSize;

	protected long batchSize;
	protected int granularity;
	protected long batchPerSlide;
	protected long numberOfBatches;
	protected StreamBatch batch;
	protected StreamBatch currentBatch;

	protected TypeSerializer<OUT> serializer;

	public BatchReduceInvokable(ReduceFunction<OUT> reduceFunction, long batchSize, long slideSize) {
		super(reduceFunction);
		this.reducer = reduceFunction;
		this.batchSize = batchSize;
		this.slideSize = slideSize;
		this.granularity = (int) MathUtils.gcd(batchSize, slideSize);
		this.batchPerSlide = slideSize / granularity;
		this.numberOfBatches = batchSize / granularity;
	}

	@Override
	protected void immutableInvoke() throws Exception {
		if ((reuse = recordIterator.next(reuse)) == null) {
			throw new RuntimeException("DataStream must not be empty");
		}

		while (reuse != null) {
			StreamBatch batch = getBatch(reuse);

			batch.reduceToBuffer(reuse.getObject());

			resetReuse();
			reuse = recordIterator.next(reuse);
		}

		reduceLastBatch();

	}

	@Override
	protected void mutableInvoke() throws Exception {
		System.out.println("Immutable setting is used");
		immutableInvoke();
	}

	protected StreamBatch getBatch(StreamRecord<OUT> next) {
		return batch;
	}

	protected void reduce(StreamBatch batch) {
		this.currentBatch = batch;
		callUserFunctionAndLogException();
	}

	protected void reduceLastBatch() throws Exception {
		batch.reduceLastBatch();
	}

	@Override
	protected void callUserFunction() throws Exception {
		Iterator<OUT> reducedIterator = currentBatch.getIterator();
		OUT reduced = null;

		while (reducedIterator.hasNext() && reduced == null) {
			reduced = reducedIterator.next();
		}

		while (reducedIterator.hasNext()) {
			OUT next = reducedIterator.next();
			if (next != null) {
				reduced = reducer.reduce(serializer.copy(reduced), serializer.copy(next));
			}
		}
		if (reduced != null) {
			collector.collect(reduced);
		}
	}

	protected class StreamBatch implements Serializable {

		private static final long serialVersionUID = 1L;
		protected long counter;
		protected long minibatchCounter;
		protected OUT currentValue;
		boolean changed;

		protected NullableCircularBuffer circularBuffer;

		public StreamBatch() {

			this.circularBuffer = new NullableCircularBuffer((int) (batchSize / granularity));
			this.counter = 0;
			this.minibatchCounter = 0;
			this.changed = false;
		}

		public void reduceToBuffer(OUT nextValue) throws Exception {

			if (currentValue != null) {
				currentValue = reducer.reduce(serializer.copy(currentValue), serializer.copy(nextValue));
			} else {
				currentValue = nextValue;
			}

			counter++;

			if (miniBatchEnd()) {
				addToBuffer();
				if (batchEnd()) {
					reduceBatch();
				}
			}

		}

		protected void addToBuffer() {
			circularBuffer.add(currentValue);
			changed = true;
			minibatchCounter++;
			currentValue = null;
		}

		protected boolean miniBatchEnd() {
			if ((counter % granularity) == 0) {
				counter = 0;
				return true;
			} else {
				return false;
			}
		}

		public boolean batchEnd() {
			if (minibatchCounter == numberOfBatches) {
				minibatchCounter -= batchPerSlide;
				return true;
			}
			return false;
		}

		public void reduceLastBatch() throws Exception {

			if (miniBatchInProgress()) {
				addToBuffer();
			}
			if (changed == true && minibatchCounter >= 0) {
				if (circularBuffer.isFull()) {
					for (long i = 0; i < (numberOfBatches - minibatchCounter); i++) {
						if (!circularBuffer.isEmpty()) {
							circularBuffer.remove();
						}
					}
				}
				if (!circularBuffer.isEmpty()) {
					reduce(this);
				}
			}
		}

		public boolean miniBatchInProgress() {
			return currentValue != null;
		}

		public void reduceBatch() {
			reduce(this);
			changed = false;
		}

		@SuppressWarnings("unchecked")
		public Iterator<OUT> getIterator() {
			return circularBuffer.iterator();
		}

		@Override
		public String toString() {
			return circularBuffer.toString();
		}

	}


	@Override
	public void open(Configuration config) throws Exception{
		super.open(config);
		serializer = inSerializer.getObjectSerializer();
		this.batch = new StreamBatch();
	}

}
