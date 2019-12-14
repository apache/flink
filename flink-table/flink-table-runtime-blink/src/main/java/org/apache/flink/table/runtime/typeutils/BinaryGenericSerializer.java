/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.runtime.util.SegmentsUtil;

import java.io.IOException;

/**
 * Serializer for {@link BinaryGeneric}.
 */
@Internal
public final class BinaryGenericSerializer<T> extends TypeSerializer<BinaryGeneric<T>> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> serializer;

	public BinaryGenericSerializer(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public BinaryGeneric<T> createInstance() {
		return new BinaryGeneric<>(serializer.createInstance());
	}

	@Override
	public BinaryGeneric<T> copy(BinaryGeneric<T> from) {
		from.ensureMaterialized(serializer);
		byte[] bytes = SegmentsUtil.copyToBytes(from.getSegments(), from.getOffset(), from.getSizeInBytes());
		T newJavaObject = from.getJavaObject() == null ? null : serializer.copy(from.getJavaObject());
		return new BinaryGeneric<>(
			new MemorySegment[]{MemorySegmentFactory.wrap(bytes)},
			0,
			bytes.length,
			newJavaObject);
	}

	@Override
	public BinaryGeneric<T> copy(BinaryGeneric<T> from, BinaryGeneric<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BinaryGeneric<T> record, DataOutputView target) throws IOException {
		record.ensureMaterialized(serializer);
		target.writeInt(record.getSizeInBytes());
		SegmentsUtil.copyToView(record.getSegments(), record.getOffset(), record.getSizeInBytes(), target);
	}

	@Override
	public BinaryGeneric<T> deserialize(DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		return new BinaryGeneric<>(
				new MemorySegment[] {MemorySegmentFactory.wrap(bytes)},
				0,
				bytes.length);
	}

	@Override
	public BinaryGeneric<T> deserialize(BinaryGeneric<T> record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public BinaryGenericSerializer<T> duplicate() {
		return new BinaryGenericSerializer<>(serializer.duplicate());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BinaryGenericSerializer that = (BinaryGenericSerializer) o;

		return serializer.equals(that.serializer);
	}

	@Override
	public int hashCode() {
		return serializer.hashCode();
	}

	@Override
	public TypeSerializerSnapshot<BinaryGeneric<T>> snapshotConfiguration() {
		return new BinaryGenericSerializerSnapshot<>(this);
	}

	public TypeSerializer<T> getInnerSerializer() {
		return serializer;
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryGenericSerializer}.
	 */
	public static final class BinaryGenericSerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<BinaryGeneric<T>, BinaryGenericSerializer<T>> {

		@SuppressWarnings("unused")
		public BinaryGenericSerializerSnapshot() {
			super(BinaryGenericSerializer.class);
		}

		public BinaryGenericSerializerSnapshot(BinaryGenericSerializer<T> serializerInstance) {
			super(serializerInstance);
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return 0;
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(BinaryGenericSerializer<T> outerSerializer) {
			return new TypeSerializer[]{outerSerializer.serializer};
		}

		@Override
		@SuppressWarnings("unchecked")
		protected BinaryGenericSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			return new BinaryGenericSerializer<>((TypeSerializer<T>) nestedSerializers[0]);
		}
	}
}
