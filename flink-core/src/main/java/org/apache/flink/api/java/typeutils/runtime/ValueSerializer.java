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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.LinkedHashMap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;

import com.esotericsoftware.kryo.Kryo;
import org.apache.flink.util.Preconditions;
import org.objenesis.strategy.StdInstantiatorStrategy;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serializer for {@link Value} types. Uses the value's serialization methods, and uses
 * Kryo for deep object copies.
 *
 * @param <T> The type serialized.
 */
@Internal
public final class ValueSerializer<T extends Value> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;
	
	private final Class<T> type;

	/**
	 * Map of class tag (using classname as tag) to their Kryo registration.
	 *
	 * <p>This map serves as a preview of the final registration result of
	 * the Kryo instance, taking into account registration overwrites.
	 *
	 * <p>Currently, we only have one single registration for the value type.
	 * Nevertheless, we keep this information here for future compatibility.
	 */
	private LinkedHashMap<String, KryoRegistration> kryoRegistrations;

	private transient Kryo kryo;
	
	private transient T copyInstance;
	
	// --------------------------------------------------------------------------------------------
	
	public ValueSerializer(Class<T> type) {
		this.type = checkNotNull(type);
		this.kryoRegistrations = asKryoRegistrations(type);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public ValueSerializer<T> duplicate() {
		return new ValueSerializer<T>(type);
	}
	
	@Override
	public T createInstance() {
		return InstantiationUtil.instantiate(this.type);
	}

	@Override
	public T copy(T from) {
		checkKryoInitialized();

		return KryoUtils.copy(from, kryo, this);
	}
	
	@Override
	public T copy(T from, T reuse) {
		checkKryoInitialized();

		return KryoUtils.copy(from, reuse, kryo, this);
	}

	@Override
	public int getLength() {
		return -1;
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
		if (this.copyInstance == null) {
			this.copyInstance = InstantiationUtil.instantiate(type);
		}
		
		this.copyInstance.read(source);
		this.copyInstance.write(target);
	}
	
	private void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();

			Kryo.DefaultInstantiatorStrategy instantiatorStrategy = new Kryo.DefaultInstantiatorStrategy();
			instantiatorStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.setInstantiatorStrategy(instantiatorStrategy);

			this.kryo.setAsmEnabled(true);

			KryoUtils.applyRegistrations(this.kryo, kryoRegistrations.values());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return this.type.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ValueSerializer) {
			ValueSerializer<?> other = (ValueSerializer<?>) obj;

			return other.canEqual(this) && type == other.type;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ValueSerializer;
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public ValueSerializerConfigSnapshot<T> snapshotConfiguration() {
		return new ValueSerializerConfigSnapshot<>(type);
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof ValueSerializerConfigSnapshot) {
			final ValueSerializerConfigSnapshot<T> config = (ValueSerializerConfigSnapshot<T>) configSnapshot;

			if (type.equals(config.getTypeClass())) {
				// currently, simply checking the type of the value class is sufficient;
				// in the future, if there are more Kryo registrations, we should try to resolve that
				return CompatibilityResult.compatible();
			}
		}

		return CompatibilityResult.requiresMigration();
	}

	public static class ValueSerializerConfigSnapshot<T extends Value> extends KryoRegistrationSerializerConfigSnapshot<T> {

		private static final int VERSION = 1;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public ValueSerializerConfigSnapshot() {}

		public ValueSerializerConfigSnapshot(Class<T> valueTypeClass) {
			super(valueTypeClass, asKryoRegistrations(valueTypeClass));
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	// --------------------------------------------------------------------------------------------

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		// kryoRegistrations may be null if this value serializer is deserialized from an old version
		if (kryoRegistrations == null) {
			this.kryoRegistrations = asKryoRegistrations(type);
		}
	}

	private static LinkedHashMap<String, KryoRegistration> asKryoRegistrations(Class<?> type) {
		Preconditions.checkNotNull(type);

		LinkedHashMap<String, KryoRegistration> registration = new LinkedHashMap<>(1);
		registration.put(type.getClass().getName(), new KryoRegistration(type));

		return registration;
	}
}
