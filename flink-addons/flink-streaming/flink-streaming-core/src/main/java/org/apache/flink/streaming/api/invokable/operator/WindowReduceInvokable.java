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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.state.SlidingWindowState;

public class WindowReduceInvokable<IN, OUT> extends StreamReduceInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	private long windowSize;
	private long slideInterval;
	private long timeUnitInMillis;
	private transient SlidingWindowState<IN> state;
	volatile boolean isRunning;

	public WindowReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long windowSize,
			long slideInterval, long timeUnitInMillis) {
		super(reduceFunction);
		this.windowSize = windowSize;
		this.slideInterval = slideInterval;
		this.timeUnitInMillis = timeUnitInMillis;
	}

	protected void immutableInvoke() throws Exception {
		if ((reuse = loadNextRecord()) == null) {
			throw new RuntimeException("DataStream must not be empty");
		}

		while (reuse != null && !state.isFull()) {
			collectOneTimeUnit();
		}
		reduce();

		while (reuse != null) {
			for (int i = 0; i < slideInterval / timeUnitInMillis; i++) {
				collectOneTimeUnit();
			}
			reduce();
		}
	}

	private void collectOneTimeUnit() {
		ArrayList<IN> list;
		list = new ArrayList<IN>();
		long startTime = System.currentTimeMillis();

		do {
			list.add(reuse.getObject());
			resetReuse();
		} while ((reuse = loadNextRecord()) != null
				&& System.currentTimeMillis() - startTime < timeUnitInMillis);
		state.pushBack(list);
	}

	private boolean reduce() throws Exception {
		userIterator = state.forceGetIterator();
		reducer.reduce(userIterable, collector);
		return reuse != null;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.state = new SlidingWindowState<IN>(windowSize, slideInterval, timeUnitInMillis);
	}

	protected void mutableInvoke() throws Exception {
		throw new RuntimeException("Reducing mutable sliding window is not supported.");
	}
}