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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public final class CopyableValueSerializer<T extends CopyableValue<T>> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	private final Class<T> valueClass;

	private transient T instance;

	public CopyableValueSerializer(Class<T> valueClass) {
		this.valueClass = checkNotNull(valueClass);
	}

	private Class<T> getValueClass() {
		return valueClass;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public CopyableValueSerializer<T> duplicate() {
		return this;
	}

	@Override
	public T createInstance() {
		return InstantiationUtil.instantiate(this.valueClass);
	}

	@Override
	public T copy(T from) {
		return copy(from, createInstance());
	}

	@Override
	public T copy(T from, T reuse) {
		from.copyTo(reuse);
		return reuse;
	}

	@Override
	public int getLength() {
		ensureInstanceInstantiated();
		return instance.getBinaryLength();
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		value.write(target);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		return deserialize(createInstance(), source);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		ensureInstanceInstantiated();
		instance.copy(source, target);
	}

	// --------------------------------------------------------------------------------------------

	private void ensureInstanceInstantiated() {
		if (instance == null) {
			instance = createInstance();
		}
	}

	@Override
	public int hashCode() {
		return this.valueClass.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CopyableValueSerializer) {
			@SuppressWarnings("unchecked")
			CopyableValueSerializer<T> copyableValueSerializer = (CopyableValueSerializer<T>) obj;

			return valueClass == copyableValueSerializer.valueClass;
		} else {
			return false;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<T> snapshotConfiguration() {
		return new CopyableValueSerializerSnapshot<>(valueClass);
	}

	@Deprecated
	public static final class CopyableValueSerializerConfigSnapshot<T extends CopyableValue<T>>
		extends GenericTypeSerializerConfigSnapshot<T> {

		private static final int VERSION = 1;

		/**
		 * This empty nullary constructor is required for deserializing the configuration.
		 */
		public CopyableValueSerializerConfigSnapshot() {
		}

		public CopyableValueSerializerConfigSnapshot(Class<T> copyableValueClass) {
			super(copyableValueClass);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
			return new CopyableValueSerializerSnapshot<>(getTypeClass())
				.resolveSchemaCompatibility(newSerializer);
		}
	}

	/**
	 * {@link TypeSerializerSnapshot} for the {@code CopyableValueSerializer}.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class CopyableValueSerializerSnapshot<T extends CopyableValue<T>>
		extends GenericTypeSerializerSnapshot<T, CopyableValueSerializer> {

		/**
		 * Used for reflective instantiation.
		 */
		@SuppressWarnings("unused")
		public CopyableValueSerializerSnapshot() {
		}

		CopyableValueSerializerSnapshot(Class<T> typeClass) {
			super(typeClass);
		}

		@Override
		protected TypeSerializer<T> createSerializer(Class<T> typeClass) {
			return new CopyableValueSerializer<>(typeClass);
		}

		@Override
		@SuppressWarnings("unchecked")
		protected Class<T> getTypeClass(CopyableValueSerializer serializer) {
			return serializer.getValueClass();
		}

		@Override
		protected Class<?> serializerClass() {
			return CopyableValueSerializer.class;
		}
	}
}
