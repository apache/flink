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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistrationSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer.PojoSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.avro.typeutils.AvroSerializer.AvroSchemaSerializerConfigSnapshot;
import org.apache.flink.formats.avro.typeutils.AvroSerializer.AvroSerializerConfigSnapshot;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An Avro serializer that can switch back to a KryoSerializer or a Pojo Serializer, if
 * it has to ensure compatibility with one of those.
 *
 * <p>This serializer is there only as a means to explicitly fall back to PoJo serialization
 * in the case where an upgrade from an earlier savepoint was made.
 *
 * @param <T> The type to be serialized.
 */
@SuppressWarnings("deprecation")
public class BackwardsCompatibleAvroSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	/** The type to serialize. */
	private final Class<T> type;

	/** The type serializer currently used. Avro by default. */
	private TypeSerializer<T> serializer;

	/**
	 * Creates a new backwards-compatible Avro Serializer, for the given type.
	 */
	public BackwardsCompatibleAvroSerializer(Class<T> type) {
		this.type = type;
		this.serializer = new AvroSerializer<>(type);
	}

	/**
	 * Private copy constructor.
	 */
	private BackwardsCompatibleAvroSerializer(Class<T> type, TypeSerializer<T> serializer) {
		this.type = type;
		this.serializer = serializer;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return serializer.isImmutableType();
	}

	@Override
	public int getLength() {
		return serializer.getLength();
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	@Override
	public T createInstance() {
		return serializer.createInstance();
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		serializer.serialize(value, target);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		return serializer.deserialize(source);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return serializer.deserialize(reuse, source);
	}

	// ------------------------------------------------------------------------
	//  Copying
	// ------------------------------------------------------------------------

	@Override
	public T copy(T from) {
		return serializer.copy(from);
	}

	@Override
	public T copy(T from, T reuse) {
		return serializer.copy(from, reuse);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serializer.copy(source, target);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public TypeSerializer<T> duplicate() {
		return new BackwardsCompatibleAvroSerializer<>(type, serializer.duplicate());
	}

	@Override
	public int hashCode() {
		return type.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj != null && obj.getClass() == BackwardsCompatibleAvroSerializer.class) {
			final BackwardsCompatibleAvroSerializer that = (BackwardsCompatibleAvroSerializer) obj;
			return this.type == that.type && this.serializer.equals(that.serializer);
		}
		else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj.getClass() == this.getClass();
	}

	@Override
	public String toString() {
		return getClass().getName() + " (" + type.getName() + ')';
	}

	// ------------------------------------------------------------------------
	//  Configuration Snapshots and Upgrades
	// ------------------------------------------------------------------------

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		// we return the configuration of the actually used serializer here
		return serializer.snapshotConfiguration();
	}

	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof AvroSchemaSerializerConfigSnapshot ||
				configSnapshot instanceof AvroSerializerConfigSnapshot) {

			// avro serializer, nice :-)
			checkState(serializer instanceof AvroSerializer,
					"Serializer was changed backwards to PojoSerializer and now encounters AvroSerializer snapshot.");

			return serializer.ensureCompatibility(configSnapshot);
		}
		else if (configSnapshot instanceof PojoSerializerConfigSnapshot) {
			// common previous case
			checkState(SpecificRecordBase.class.isAssignableFrom(type),
					"BackwardsCompatibleAvroSerializer resuming a state serialized " +
							"via a PojoSerializer, but not for an Avro Specific Record");

			final AvroTypeInfo<? extends SpecificRecordBase> typeInfo =
					new AvroTypeInfo<>(type.asSubclass(SpecificRecordBase.class), true);

			@SuppressWarnings("unchecked")
			final TypeSerializer<T> pojoSerializer =
					(TypeSerializer<T>) typeInfo.createPojoSerializer(new ExecutionConfig());
			this.serializer = pojoSerializer;
			return serializer.ensureCompatibility(configSnapshot);
		}
		else if (configSnapshot instanceof KryoRegistrationSerializerConfigSnapshot) {
			// force-kryo old case common previous case
			// we create a new Kryo Serializer with a blank execution config.
			// registrations are anyways picked up from the snapshot.
			serializer = new KryoSerializer<>(type, new ExecutionConfig());
			return serializer.ensureCompatibility(configSnapshot);
		}
		else {
			// completely incompatible type, needs migration
			return CompatibilityResult.requiresMigration();
		}
	}
}
