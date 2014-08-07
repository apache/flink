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

import org.apache.commons.math.util.MathUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.state.SlidingWindowState;

public class BatchReduceInvokable<IN, OUT> extends StreamReduceInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	private int batchSize;
	private int slideSize;
	private int granularity;
	private boolean emitted;
	private transient SlidingWindowState<IN> state;

	public BatchReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, int batchSize,
			int slideSize) {
		super(reduceFunction);		
		this.reducer = reduceFunction;
		this.batchSize = batchSize;
		this.slideSize = slideSize;
		this.granularity = MathUtils.gcd(batchSize, slideSize);
	}

	@Override
	protected void immutableInvoke() throws Exception {
		reuse = loadNextRecord();
		ArrayList<IN> list;

		while (!state.isFull()) {
			list = new ArrayList<IN>(granularity);
			try {
				state.pushBack(fillArray(list));
			} catch (NullPointerException e) {
				throw new RuntimeException("DataStream length must be greater than batchsize");
			}
		}

		boolean go = reduce();

		while (go) {
			if (state.isEmittable()) {
				go = reduce();
			} else {
				list = (ArrayList<IN>) state.popFront();
				list.clear();
				state.pushBack(fillArray(list));
				emitted = false;
				go = reuse != null;
			}
		}
		if (!emitted) {
			reduce();
		}
	}

	private boolean reduce() throws Exception {
		userIterator = state.getIterator();
		reducer.reduce(userIterable, collector);
		emitted = true;
		return reuse != null;
	}

	private ArrayList<IN> fillArray(ArrayList<IN> list) {
		int counter = 0;
		do {
			counter++;
			list.add(reuse.getObject());
			resetReuse();
		} while ((reuse = loadNextRecord()) != null && counter < granularity);
		return list;
	}

	@Override
	protected void mutableInvoke() throws Exception {
		throw new RuntimeException("Reducing mutable sliding batch is not supported.");
	}
	
	@Override
	public void open(Configuration parameters) throws Exception{
		super.open(parameters);
		this.state = new SlidingWindowState<IN>(batchSize, slideSize, granularity);
	}

}