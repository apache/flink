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

import java.util.Iterator;

import org.apache.flink.api.java.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;

public abstract class StreamReduceInvokable<IN, OUT> extends UserTaskInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	protected RichGroupReduceFunction<IN, OUT> reducer;
	protected BatchIterator<IN> userIterator;
	protected BatchIterable userIterable;

	@Override
	public void open(Configuration parameters) throws Exception {
		userIterable = new BatchIterable();
		reducer.open(parameters);
	}

	@Override
	public void close() throws Exception {
		reducer.close();
	}

	protected class BatchIterable implements Iterable<IN> {

		@Override
		public Iterator<IN> iterator() {
			return userIterator;
		}

	}
}