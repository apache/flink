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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.util.InstantiationUtil;

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
		return new BinaryGeneric<>(serializer.createInstance(), serializer);
	}

	@Override
	public BinaryGeneric<T> copy(BinaryGeneric<T> from) {
		return from.copy();
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
				0, bytes.length, serializer);
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
		return this;
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
		return new BinaryGenericSerializerSnapshot<>(serializer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryGenericSerializer}.
	 */
	public static final class BinaryGenericSerializerSnapshot<T> implements TypeSerializerSnapshot<BinaryGeneric<T>> {
		private static final int CURRENT_VERSION = 3;

		private TypeSerializer<T> previousSerializer;

		@SuppressWarnings("unused")
		public BinaryGenericSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BinaryGenericSerializerSnapshot(TypeSerializer<T> serializer) {
			this.previousSerializer = serializer;
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			InstantiationUtil.serializeObject(new DataOutputViewStream(out), previousSerializer);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			try {
				this.previousSerializer = InstantiationUtil.deserializeObject(
						new DataInputViewStream(in), userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		@Override
		public TypeSerializer<BinaryGeneric<T>> restoreSerializer() {
			return new BinaryGenericSerializer<>(previousSerializer);
		}

		@Override
		public TypeSerializerSchemaCompatibility<BinaryGeneric<T>> resolveSchemaCompatibility(TypeSerializer<BinaryGeneric<T>> newSerializer) {
			if (!(newSerializer instanceof BinaryGenericSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BinaryGenericSerializer newBinaryGenericSerializer = (BinaryGenericSerializer) newSerializer;
			if (!previousSerializer.equals(newBinaryGenericSerializer.serializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
