/**
 *
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
 *
 */

package org.apache.flink.streaming.api.invokable.operator;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.state.SlidingWindowState;

public abstract class StreamReduceInvokable<IN, OUT> extends UserTaskInvokable<IN, OUT> {

	public StreamReduceInvokable(Function userFunction) {
		super(userFunction);
	}

	private static final long serialVersionUID = 1L;
	protected GroupReduceFunction<IN, OUT> reducer;
	protected BatchIterator<IN> userIterator;
	protected BatchIterable userIterable;
	protected long slideSize;
	protected long granularity;
	protected int listSize;
	protected transient SlidingWindowState<IN> state;

	@Override
	public void open(Configuration parameters) throws Exception {
		userIterable = new BatchIterable();
		super.open(parameters);
	}

	@Override
	protected void immutableInvoke() throws Exception {
		if ((reuse = loadNextRecord()) == null) {
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

	protected void reduce() {
		userIterator = state.getIterator();
		callUserFunctionAndLogException();
	}
	
	@Override
	protected void callUserFunction() throws Exception {
		reducer.reduce(userIterable, collector);
	}
	
	private void collectOneUnit() {
		ArrayList<StreamRecord<IN>> list;
		list = new ArrayList<StreamRecord<IN>>(listSize);

		do {
			list.add(reuse);
			resetReuse();
		} while ((reuse = loadNextRecord()) != null && batchNotFull());
		state.pushBack(list);
	}

	protected abstract boolean batchNotFull();

	protected class BatchIterable implements Iterable<IN> {

		@Override
		public Iterator<IN> iterator() {
			return userIterator;
		}

	}
}