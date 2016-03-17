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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayDeque;

/**
 * An {@link EvictingWindowBuffer} that stores elements on the Java Heap.
 *
 * @param <T> The type of elements that this {@code WindowBuffer} can store.
 */
@Internal
public class ListWindowBuffer<T> implements EvictingWindowBuffer<T, T> {

	private final TypeSerializer<T>  serializer;

	private ArrayDeque<StreamRecord<T>> elements;

	protected ListWindowBuffer(TypeSerializer<T> serializer) {
		this.serializer = serializer;
		this.elements = new ArrayDeque<>();
	}

	protected ListWindowBuffer(ArrayDeque<StreamRecord<T>> elements, TypeSerializer<T> serializer) {
		this.serializer = serializer;
		this.elements = elements;
	}

	@Override
	public void storeElement(StreamRecord<T> element) {
		elements.add(element);
	}

	@Override
	public void removeElements(int count) {
		// TODO determine if this can be done in a better way
		for (int i = 0; i < count; i++) {
			elements.removeFirst();
		}
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

	@Override
	public void snapshot(DataOutputView out) throws IOException {
		out.writeInt(elements.size());

		MultiplexingStreamRecordSerializer<T> recordSerializer = new MultiplexingStreamRecordSerializer<>(serializer);

		for (StreamRecord<T> e: elements) {
			recordSerializer.serialize(e, out);
		}
	}

	public static class Factory<T> implements WindowBufferFactory<T, T, ListWindowBuffer<T>> {
		private static final long serialVersionUID = 1L;

		private final TypeSerializer<T> serializer;

		public Factory(TypeSerializer<T> serializer) {
			this.serializer = serializer;
		}

		@Override
		public ListWindowBuffer<T> create() {
			return new ListWindowBuffer<>(serializer);
		}

		@Override
		public ListWindowBuffer<T> restoreFromSnapshot(DataInputView in) throws IOException {
			int size = in.readInt();

			MultiplexingStreamRecordSerializer<T> recordSerializer = new MultiplexingStreamRecordSerializer<>(serializer);

			ArrayDeque<StreamRecord<T>> elements = new ArrayDeque<>();

			for (int i = 0; i < size; i++) {
				elements.add(recordSerializer.deserialize(in).<T>asRecord());
			}

			return new ListWindowBuffer<>(elements, serializer);
		}
	}
}
