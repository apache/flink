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
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;

public class BatchReduceInvokable<IN, OUT> extends StreamReduceInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	private int batchSize;

	public BatchReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, int batchSize) {
		super(reduceFunction);
		this.reducer = reduceFunction;
		this.batchSize = batchSize;
	}

	@Override
	protected void immutableInvoke() throws Exception {
		List<IN> tupleBatch = new ArrayList<IN>();
		boolean batchStart;
		int counter = 0;

		while (loadNextRecord() != null) {
			batchStart = true;
			do {
				if (batchStart) {
					batchStart = false;
				} else {
					reuse = loadNextRecord();
					if (reuse == null) {
						break;
					}
				}
				counter++;
				tupleBatch.add(reuse.getObject());
				resetReuse();
			} while (counter < batchSize);
			reducer.reduce(tupleBatch, collector);
			tupleBatch.clear();
			counter = 0;
		}

	}

	@Override
	protected void mutableInvoke() throws Exception {
		userIterator = new CounterIterator();

		do {
			if (userIterator.hasNext()) {
				reducer.reduce(userIterable, collector);
				userIterator.reset();
			}
		} while (reuse != null);
	}

	private class CounterIterator implements BatchIterator<IN> {
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
				return reuse.getObject();
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

}