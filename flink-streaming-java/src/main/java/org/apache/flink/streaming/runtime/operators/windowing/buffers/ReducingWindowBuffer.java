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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Collections;

/**
 * An {@link WindowBuffer} that stores elements on the Java Heap. This buffer uses a
 * {@link ReduceFunction} to incrementally aggregate elements that are added to the buffer.
 *
 * @param <T> The type of elements that this {@code WindowBuffer} can store.
 */
@Internal
public class ReducingWindowBuffer<T> implements WindowBuffer<T, T> {

	private final ReduceFunction<T> reduceFunction;
	private final TypeSerializer<T> serializer;
	private  StreamRecord<T> data;

	protected ReducingWindowBuffer(ReduceFunction<T> reduceFunction, TypeSerializer<T> serializer) {
		this.reduceFunction = reduceFunction;
		this.serializer = serializer;
		this.data = null;
	}

	protected ReducingWindowBuffer(ReduceFunction<T> reduceFunction, StreamRecord<T> data, TypeSerializer<T> serializer) {
		this.reduceFunction = reduceFunction;
		this.serializer = serializer;
		this.data = data;
	}

	@Override
	public void storeElement(StreamRecord<T> element) throws Exception {
		if (data == null) {
			data = element.copy(element.getValue());
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

	@Override
	public void snapshot(DataOutputView out) throws IOException {
		if (data != null) {
			out.writeBoolean(true);
			MultiplexingStreamRecordSerializer<T> recordSerializer = new MultiplexingStreamRecordSerializer<>(serializer);
			recordSerializer.serialize(data, out);
		} else {
			out.writeBoolean(false);
		}
	}

	public static class Factory<T> implements WindowBufferFactory<T, T, ReducingWindowBuffer<T>> {
		private static final long serialVersionUID = 1L;

		private final ReduceFunction<T> reduceFunction;

		private final TypeSerializer<T> serializer;

		public Factory(ReduceFunction<T> reduceFunction, TypeSerializer<T> serializer) {
			this.reduceFunction = reduceFunction;
			this.serializer = serializer;
		}

		@Override
		public ReducingWindowBuffer<T> create() {
			return new ReducingWindowBuffer<>(reduceFunction, serializer);
		}

		@Override
		public ReducingWindowBuffer<T> restoreFromSnapshot(DataInputView in) throws IOException {
			boolean hasValue = in.readBoolean();
			if (hasValue) {
				MultiplexingStreamRecordSerializer<T> recordSerializer = new MultiplexingStreamRecordSerializer<>(serializer);
				StreamElement element = recordSerializer.deserialize(in);
				return new ReducingWindowBuffer<>(reduceFunction, element.<T>asRecord(), serializer);
			} else {
				return new ReducingWindowBuffer<>(reduceFunction, serializer);
			}
		}
	}
}
