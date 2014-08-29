/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.math.util.MathUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.SlidingWindowState;

public class BatchReduceInvokable<IN, OUT> extends UserTaskInvokable<IN, OUT> {

	private static final long serialVersionUID = 1L;
	protected GroupReduceFunction<IN, OUT> reducer;
	protected BatchIterator<IN> userIterator;
	protected Iterable<IN> userIterable;
	protected long slideSize;
	protected long granularity;
	protected int listSize;
	protected transient SlidingWindowState<IN> state;
	
	private long batchSize;
	private int counter = 0;

	public BatchReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long batchSize,
			long slideSize) {
		super(reduceFunction);
		this.reducer = reduceFunction;
		this.batchSize = batchSize;
		this.slideSize = slideSize;
		this.granularity = MathUtils.gcd(batchSize, slideSize);
		this.listSize = (int) granularity;
	}

	@Override
	protected void mutableInvoke() throws Exception {
		throw new RuntimeException("Reducing mutable sliding batch is not supported.");
	}

	@Override
	protected void immutableInvoke() throws Exception {
		if ((reuse = recordIterator.next(reuse)) == null) {
			throw new RuntimeException("DataStream must not be empty");
		}
	
		while (reuse != null && !state.isFull()) {
			collectOneUnit();
		}
		reduce();
	
		while (reuse != null) {
			for (int i = 0; i < slideSize / granularity; i++) {
				if (reuse != null) {
					collectOneUnit();
				}
			}
			reduce();
		}
	}

	private void collectOneUnit() throws IOException {
		ArrayList<StreamRecord<IN>> list;
		list = new ArrayList<StreamRecord<IN>>(listSize);
	
		do {
			list.add(reuse);
			resetReuse();
		} while ((reuse = recordIterator.next(reuse)) != null && batchNotFull());
		state.pushBack(list);
	}

	
	protected boolean batchNotFull() {
		counter++;
		if (counter < granularity) {
			return true;
		} else {
			counter = 0;
			return false;
		}
	}

	protected void reduce() {
		userIterator = state.getIterator();
		callUserFunctionAndLogException();
	}
	
	@Override
	protected void callUserFunction() throws Exception {
		reducer.reduce(userIterable, collector);
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.state = new SlidingWindowState<IN>(batchSize, slideSize, granularity);
		userIterable = new BatchIterable();
	}

	protected class BatchIterable implements Iterable<IN> {

		@Override
		public Iterator<IN> iterator() {
			return userIterator;
		}

	}


}