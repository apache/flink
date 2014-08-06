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

public class WindowReduceInvokable<IN, OUT> extends StreamReduceInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	private long windowSize;
	volatile boolean isRunning;
	boolean window;

	public WindowReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction, long windowSize) {
		super(reduceFunction);
		this.reducer = reduceFunction;
		this.windowSize = windowSize;
		this.window = true;
	}

	protected void immutableInvoke() throws Exception {
		List<IN> tupleBatch = new ArrayList<IN>();
		boolean batchStart;

		long startTime = System.currentTimeMillis();
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
				tupleBatch.add(reuse.getObject());
				resetReuse();
			} while (System.currentTimeMillis() - startTime < windowSize);
			reducer.reduce(tupleBatch, collector);
			tupleBatch.clear();
			startTime = System.currentTimeMillis();
		}

	}

	protected void mutableInvoke() throws Exception {
		userIterator = new WindowIterator();

		do {
			if (userIterator.hasNext()) {
				reducer.reduce(userIterable, collector);
				userIterator.reset();
			}
		} while (reuse != null);
	}

	private class WindowIterator implements BatchIterator<IN> {

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
				return reuse.getObject();
			} else {
				loadedNext = false;
				return reuse.getObject();
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