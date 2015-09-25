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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;

public class HeapWindowBuffer<T> implements EvictingWindowBuffer<T> {
	private static final long serialVersionUID = 1L;

	private ArrayDeque<StreamRecord<T>> elements;

	protected HeapWindowBuffer() {
		this.elements = new ArrayDeque<>();
	}

	@Override
	public void storeElement(StreamRecord<T> element) {
		elements.add(element);
	}

	@Override
	public boolean removeElements(int count) {
		// TODO determine if this can be done in a better way
		for (int i = 0; i < count; i++) {
			elements.removeFirst();
		}
		return false;
	}

	@Override
	public Iterable<StreamRecord<T>> getElements() {
		return elements;
	}

	@Override
	public Iterable<T> getUnpackedElements() {
		return FluentIterable.from(elements).transform(new Function<StreamRecord<T>, T>() {
			@Override
			public T apply(StreamRecord<T> record) {
				return record.getValue();
			}
		});
	}

	@Override
	public int size() {
		return elements.size();
	}

	public static class Factory<T> implements WindowBufferFactory<T, HeapWindowBuffer<T>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void setRuntimeContext(RuntimeContext ctx) {}

		@Override
		public void open(Configuration config) {}

		@Override
		public void close() {}

		@Override
		public HeapWindowBuffer<T> create() {
			return new HeapWindowBuffer<>();
		}
	}
}
