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

import org.apache.commons.math.util.MathUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.CircularFifoList;

public class BatchGroupReduceInvokable<IN, OUT> extends StreamInvokable<IN, OUT> {

	private static final long serialVersionUID = 1L;
	protected GroupReduceFunction<IN, OUT> reducer;

	protected long slideSize;

	protected long batchSize;
	protected int granularity;
	protected int batchPerSlide;
	protected StreamBatch batch;
	protected StreamBatch currentBatch;
	protected long numberOfBatches;

	public BatchGroupReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long batchSize,
			long slideSize) {
		super(reduceFunction);
		this.reducer = reduceFunction;
		this.batchSize = batchSize;
		this.slideSize = slideSize;
		this.granularity = (int) MathUtils.gcd(batchSize, slideSize);
		this.batchPerSlide = (int) (slideSize / granularity);
		this.numberOfBatches = batchSize / granularity;
		this.batch = new StreamBatch();
	}

	@Override
	protected void immutableInvoke() throws Exception {
		if ((reuse = recordIterator.next(reuse)) == null) {
			throw new RuntimeException("DataStream must not be empty");
		}

		while (reuse != null) {
			StreamBatch batch = getBatch(reuse);
			batch.addToBuffer(reuse.getObject());

			resetReuse();
			reuse = recordIterator.next(reuse);
		}

		reduceLastBatch();
	}

	@Override
	// TODO: implement mutableInvoke for reduce
	protected void mutableInvoke() throws Exception {
		System.out.println("Immutable setting is used");
		immutableInvoke();
	}

	protected StreamBatch getBatch(StreamRecord<IN> next) {
		return batch;
	}

	protected void reduce(StreamBatch batch) {
		this.currentBatch = batch;
		callUserFunctionAndLogException();
	}

	protected void reduceLastBatch() {
		batch.reduceLastBatch();
	}

	@Override
	protected void callUserFunction() throws Exception {
		if(!currentBatch.circularList.isEmpty()){
			reducer.reduce(currentBatch.circularList.getIterable(), collector);
		}
	}

	protected class StreamBatch implements Serializable {

		private static final long serialVersionUID = 1L;
		private long counter;
		protected long minibatchCounter;

		protected CircularFifoList<IN> circularList;

		public StreamBatch() {
			this.circularList = new CircularFifoList<IN>();
			this.counter = 0;
			this.minibatchCounter = 0;
		}

		public void addToBuffer(IN nextValue) throws Exception {
			circularList.add(nextValue);

			counter++;

			if (miniBatchEnd()) {
				circularList.newSlide();
				minibatchCounter++;
				if (batchEnd()) {
					reduceBatch();
					circularList.shiftWindow(batchPerSlide);
				}
			}

		}

		protected boolean miniBatchEnd() {
			if( (counter % granularity) == 0){
				counter = 0;
				return true;
			}else{
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

		public void reduceBatch() {
			reduce(this);
		}

		public void reduceLastBatch() {
			if (!miniBatchEnd()) {
				reduceBatch();
			}
		}
		
		@Override
		public String toString(){
			return circularList.toString();
		}

	}

}
