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

import java.util.Iterator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;

public class BatchReduceInvokable<OUT> extends BatchGroupReduceInvokable<OUT, OUT> {

	private static final long serialVersionUID = 1L;
	protected ReduceFunction<OUT> reducer;
	protected TypeSerializer<OUT> typeSerializer;
	protected OUT reduceReuse;

	public BatchReduceInvokable(ReduceFunction<OUT> reduceFunction, long batchSize, long slideSize) {
		super(null, batchSize, slideSize);
		this.reducer = reduceFunction;
	}

	protected void collectOneUnit() throws Exception {
		OUT reduced = null;
		if (batchNotFull()) {
			reduced = reuse.getObject();
			resetReuse();
			while (getNextRecord() != null && batchNotFull()) {
				reduced = reducer.reduce(reduced, reuse.getObject());
				resetReuse();
			}
		}
		state.pushBack(reduced);
	}

	@Override
	protected void reduce() {
		callUserFunctionAndLogException();
	}

	@Override
	protected void callUserFunction() throws Exception {
		Iterator<OUT> reducedIterator = state.getBufferIterator();
		OUT reduced;
		do {
			reduced = reducedIterator.next();
		} while (reducedIterator.hasNext() && reduced == null);

		while (reducedIterator.hasNext()) {
			OUT next = reducedIterator.next();
			if (next != null) {
				next = typeSerializer.copy(next, reduceReuse);
				reduced = reducer.reduce(reduced, next);
			}
		}
		collector.collect(reduced);
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		this.typeSerializer = serializer.getObjectSerializer();
		this.reduceReuse = typeSerializer.createInstance();
	}
}