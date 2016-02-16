/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.operators.windowing.buffers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collections;

/**
 * An {@link WindowBuffer} that stores elements on the Java Heap. This buffer uses a
 * {@link ReduceFunction} to pre-aggregate elements that are added to the buffer.
 *
 * @param <T> The type of elements that this {@code WindowBuffer} can store.
 */
@Internal
public class PreAggregatingHeapWindowBuffer<T> implements WindowBuffer<T> {
	private static final long serialVersionUID = 1L;

	private final ReduceFunction<T> reduceFunction;
	private transient StreamRecord<T> data;

	protected PreAggregatingHeapWindowBuffer(ReduceFunction<T> reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	@Override
	public void storeElement(StreamRecord<T> element) throws Exception {
		if (data == null) {
			data = new StreamRecord<>(element.getValue(), element.getTimestamp());
		} else {
			data.replace(reduceFunction.reduce(data.getValue(), element.getValue()));
		}
	}

	@Override
	public Iterable<StreamRecord<T>> getElements() {
		return Collections.singleton(data);
	}

	@Override
	public Iterable<T> getUnpackedElements() {
		return Collections.singleton(data.getValue());
	}

	@Override
	public int size() {
		return 1;
	}

	public static class Factory<T> implements WindowBufferFactory<T, PreAggregatingHeapWindowBuffer<T>> {
		private static final long serialVersionUID = 1L;

		private final ReduceFunction<T> reduceFunction;

		public Factory(ReduceFunction<T> reduceFunction) {
			this.reduceFunction = reduceFunction;
		}

		@Override
		public void setRuntimeContext(RuntimeContext ctx) {
			FunctionUtils.setFunctionRuntimeContext(reduceFunction, ctx);
		}

		@Override
		public void open(Configuration config) throws Exception {
			FunctionUtils.openFunction(reduceFunction, config);
		}

		@Override
		public void close() throws Exception {
			FunctionUtils.closeFunction(reduceFunction);
		}

		@Override
		public PreAggregatingHeapWindowBuffer<T> create() {
			return new PreAggregatingHeapWindowBuffer<>(reduceFunction);
		}
	}
}
