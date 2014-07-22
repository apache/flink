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

import java.io.IOException;

import org.apache.flink.api.java.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class BatchReduceInvokable<IN extends Tuple, OUT extends Tuple> extends
		UserTaskInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	private int batchSize;
	private long windowSize;
	volatile boolean isRunning;
	boolean window;

	private GroupReduceFunction<IN, OUT> reducer;

	public BatchReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, int batchSize) {
		this.reducer = reduceFunction;
		this.batchSize = batchSize;
	}

	public BatchReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long windowSize) {
		this.reducer = reduceFunction;
		this.windowSize = windowSize;
		this.window = true;
	}

	@Override
	public void invoke() throws Exception {
		BatchIterator<IN> userIterator;
		if (window) {
			userIterator = new WindowIterator();
		} else {
			userIterator = new CounterIterator();
		}

		do {
			if (userIterator.hasNext()) {
				reducer.reduce(userIterator, collector);
				userIterator.reset();
			}
		} while (reuse != null);
	}

	private StreamRecord<IN> loadNextRecord() {
		if (!isMutable) {
			resetReuse();
		}
		try {
			reuse = recordIterator.next(reuse);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return reuse;
	}

	public class CounterIterator implements BatchIterator<IN> {
		private int counter;
		private boolean loadedNext;

		public CounterIterator() {
			counter = 1;
		}

		@Override
		public boolean hasNext() {
			if (counter > batchSize) {
				return false;
			} else if (!loadedNext) {
				loadNextRecord();
				loadedNext = true;
			}
			return (reuse != null);
		}

		@Override
		public IN next() {
			if (hasNext()) {
				counter++;
				loadedNext = false;
				return reuse.getTuple();
			} else {
				counter++;
				loadedNext = false;
				return null;
			}
		}

		public void reset() {
			for (int i = 0; i < (batchSize - counter); i++) {
				loadNextRecord();
			}
			loadNextRecord();
			loadedNext = true;
			counter = 1;
		}

		@Override
		public void remove() {

		}

	}

	public class WindowIterator implements BatchIterator<IN> {

		volatile boolean iterate;
		private boolean loadedNext;
		private long startTime;

		public WindowIterator() {
			startTime = System.currentTimeMillis();
		}

		@Override
		public boolean hasNext() {
			if (System.currentTimeMillis() - startTime > windowSize) {
				return false;
			} else if (!loadedNext) {
				loadNextRecord();
				loadedNext = true;
			}
			return (reuse != null);
		}

		@Override
		public IN next() {
			if (hasNext()) {
				loadedNext = false;
				return reuse.getTuple();
			} else {
				loadedNext = false;
				return reuse.getTuple();
			}
		}

		public void reset() {
			while (System.currentTimeMillis() - startTime < windowSize) {
				loadNextRecord();
			}
			loadNextRecord();
			loadedNext = true;
			startTime = System.currentTimeMillis();
		}

		@Override
		public void remove() {

		}

	}

}