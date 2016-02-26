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
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;

/**
 * An {@link WindowBuffer} that stores elements on the Java Heap. This buffer uses a
 * {@link FoldFunction} to incrementally aggregate elements that are added to the buffer.
 *
 * @param <T> The type of elements that can be added to this {@code WindowBuffer}.
 * @param <ACC> The type of the accumulator that this {@code WindowBuffer} can store.
 */
@Internal
public class FoldingWindowBuffer<T, ACC> implements WindowBuffer<T, ACC> {

	private final FoldFunction<T, ACC> foldFunction;
	private final TypeSerializer<ACC> accSerializer;
	private StreamRecord<ACC> data;

	protected FoldingWindowBuffer(FoldFunction<T, ACC> foldFunction, ACC initialAccumulator, TypeSerializer<ACC> accSerializer) {
		this.foldFunction = foldFunction;
		this.accSerializer = accSerializer;
		this.data = new StreamRecord<>(initialAccumulator);
	}

	protected FoldingWindowBuffer(FoldFunction<T, ACC> foldFunction, StreamRecord<ACC> initialAccumulator, TypeSerializer<ACC> accSerializer) {
		this.foldFunction = foldFunction;
		this.accSerializer = accSerializer;
		this.data = initialAccumulator;
	}

	@Override
	public void storeElement(StreamRecord<T> element) throws Exception {
		data.replace(foldFunction.fold(data.getValue(), element.getValue()));
	}

	@Override
	public Iterable<StreamRecord<ACC>> getElements() {
		return Collections.singleton(data);
	}

	@Override
	public Iterable<ACC> getUnpackedElements() {
		return Collections.singleton(data.getValue());
	}

	@Override
	public int size() {
		return 1;
	}

	@Override
	public void snapshot(DataOutputView out) throws IOException {
		MultiplexingStreamRecordSerializer<ACC> recordSerializer = new MultiplexingStreamRecordSerializer<>(accSerializer);
		recordSerializer.serialize(data, out);
	}

	public static class Factory<T, ACC> implements WindowBufferFactory<T, ACC, FoldingWindowBuffer<T, ACC>> {
		private static final long serialVersionUID = 1L;

		private final FoldFunction<T, ACC> foldFunction;

		private final TypeSerializer<ACC> accSerializer;

		private transient ACC initialAccumulator;

		public Factory(FoldFunction<T, ACC> foldFunction, ACC initialValue, TypeSerializer<ACC> accSerializer) {
			this.foldFunction = foldFunction;
			this.accSerializer = accSerializer;
			this.initialAccumulator = initialValue;
		}

		@Override
		public FoldingWindowBuffer<T, ACC> create() {
			return new FoldingWindowBuffer<>(foldFunction, accSerializer.copy(initialAccumulator), accSerializer);
		}

		@Override
		public FoldingWindowBuffer<T, ACC> restoreFromSnapshot(DataInputView in) throws IOException {
			MultiplexingStreamRecordSerializer<ACC> recordSerializer = new MultiplexingStreamRecordSerializer<>(accSerializer);
			StreamElement element = recordSerializer.deserialize(in);
			return new FoldingWindowBuffer<>(foldFunction, element.<ACC>asRecord(), accSerializer);
		}

		private void writeObject(final ObjectOutputStream out) throws IOException {
			// write all the non-transient fields
			out.defaultWriteObject();


			byte[] serializedDefaultValue;
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
					DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(baos))
			{
				accSerializer.serialize(initialAccumulator, outView);

				outView.flush();
				serializedDefaultValue = baos.toByteArray();
			}
			catch (Exception e) {
				throw new IOException("Unable to serialize initial accumulator of type " +
						initialAccumulator.getClass().getSimpleName() + ".", e);
			}

			out.writeInt(serializedDefaultValue.length);
			out.write(serializedDefaultValue);
		}

		private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
			// read the non-transient fields
			in.defaultReadObject();

			// read the default value field
			int size = in.readInt();
			byte[] buffer = new byte[size];
			int bytesRead = in.read(buffer);

			if (bytesRead != size) {
				throw new RuntimeException("Read size does not match expected size.");
			}

			try (ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
					DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais))
			{
				initialAccumulator = accSerializer.deserialize(inView);
			}
			catch (Exception e) {
				throw new IOException("Unable to deserialize initial accumulator.", e);
			}
		}
	}
}
