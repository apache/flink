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
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Objects;

/**
 * A serializer that can serialize and deserialize all data structures defined by a {@link DataType}.
 *
 * <p>This class combines {@link DataStructureConverters} and {@link InternalSerializers} into one
 * entity. The serialized binary format is always an internal binary format.
 *
 * <p>Serializing {@code null} in the top-level is not supported.
 *
 * @param <I> internal data structure
 * @param <E> external data structure
 */
@Internal
public final class ExternalSerializer<I, E> extends TypeSerializer<E> {

	private final DataType dataType;

	private final TypeSerializer<I> internalSerializer;

	private transient DataStructureConverter<I, E> converter;

	private ExternalSerializer(DataType dataType, TypeSerializer<I> internalSerializer) {
		this.dataType = dataType;
		this.internalSerializer = internalSerializer;
	}

	/**
	 * Creates an instance of a {@link ExternalSerializer} defined by the given {@link DataType}.
	 */
	public static <I, E> ExternalSerializer<I, E> of(DataType dataType) {
		return new ExternalSerializer<>(dataType, InternalSerializers.create(dataType.getLogicalType()));
	}

	@SuppressWarnings("unchecked")
	private void checkConverterInitialized() {
		if (converter == null) {
			converter = (DataStructureConverter<I, E>) DataStructureConverters.getConverter(dataType);
			converter.open(Thread.currentThread().getContextClassLoader());
		}
	}

	@Override
	public boolean isImmutableType() {
		return internalSerializer.isImmutableType();
	}

	@Override
	public TypeSerializer<E> duplicate() {
		return new ExternalSerializer<>(dataType, internalSerializer.duplicate());
	}

	@Override
	public E createInstance() {
		checkConverterInitialized();
		// in some cases this fails
		// e.g. for objects backed by non-existing binary sections
		try {
			I instance = internalSerializer.createInstance();
			return converter.toExternal(instance);
		} catch (Throwable t) {
			return null;
		}
	}

	@Override
	public E copy(E from) {
		checkConverterInitialized();
		final I internalFrom = converter.toInternal(from);
		final I copy = internalSerializer.copy(internalFrom);
		return converter.toExternal(copy);
	}

	@Override
	public E copy(E from, E reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return internalSerializer.getLength();
	}

	@Override
	public void serialize(E record, DataOutputView target) throws IOException {
		checkConverterInitialized();
		final I internalRecord = converter.toInternal(record);
		internalSerializer.serialize(internalRecord, target);
	}

	@Override
	public E deserialize(DataInputView source) throws IOException {
		checkConverterInitialized();
		final I internalRecord = internalSerializer.deserialize(source);
		return converter.toExternal(internalRecord);
	}

	@Override
	public E deserialize(E reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		internalSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ExternalSerializer<?, ?> that = (ExternalSerializer<?, ?>) o;
		return dataType.equals(that.dataType) &&
			internalSerializer.equals(that.internalSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dataType, internalSerializer);
	}

	@Override
	public TypeSerializerSnapshot<E> snapshotConfiguration() {
		return new ExternalSerializerSnapshot<>(this);
	}

	// ---------------------------------------------------------------------------------
	// Serializer Snapshot
	// ---------------------------------------------------------------------------------

	/**
	 * {@link TypeSerializerSnapshot} for {@link ExternalSerializer}.
	 *
	 * @param <I> internal data structure
	 * @param <E> external data structure
	 */
	public static final class ExternalSerializerSnapshot<I, E>
			extends CompositeTypeSerializerSnapshot<E, ExternalSerializer<I, E>> {

		private static final int VERSION = 1;

		private DataType dataType;

		public ExternalSerializerSnapshot() {
			super(ExternalSerializer.class);
		}

		public ExternalSerializerSnapshot(ExternalSerializer<I, E> externalSerializer) {
			super(externalSerializer);
			this.dataType = externalSerializer.dataType;
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return VERSION;
		}

		@Override
		protected void writeOuterSnapshot(DataOutputView out) throws IOException {
			final DataOutputViewStream stream = new DataOutputViewStream(out);
			InstantiationUtil.serializeObject(stream, dataType);
		}

		@Override
		protected void readOuterSnapshot(
				int readOuterSnapshotVersion,
				DataInputView in,
				ClassLoader userCodeClassLoader) throws IOException {
			final DataInputViewStream stream = new DataInputViewStream(in);
			try {
				dataType = InstantiationUtil.deserializeObject(stream, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(ExternalSerializer<I, E> outerSerializer) {
			return new TypeSerializer[]{outerSerializer.internalSerializer};
		}

		@Override
		@SuppressWarnings("unchecked")
		protected ExternalSerializer<I, E> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			return new ExternalSerializer<>(dataType, (TypeSerializer<I>) nestedSerializers[0]);
		}
	}
}
