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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.LinkedOptionalMap;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.LinkedOptionalMap.optionalMapOf;
import static org.apache.flink.util.LinkedOptionalMapSerializer.readOptionalMap;
import static org.apache.flink.util.LinkedOptionalMapSerializer.writeOptionalMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class holds the snapshot content for the {@link PojoSerializer}.
 *
 * <h2>Serialization Format</hr>
 *
 * <p>The serialization format defined by this class is as follows:
 *
 * <pre>{@code
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                            POJO class name                                          |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |      number of fields      |                (field name, field serializer snapshot)                 |
 * |                            |                                pairs                                   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |         number of          |       (registered subclass name, subclass serializer snapshot)         |
 * |   registered subclasses    |                                pairs                                   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |         number of          |     (non-registered subclass name, subclass serializer snapshot)       |
 * | non-registered subclasses  |                                pairs                                   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }</pre>
 */
@Internal
final class PojoSerializerSnapshotData<T> {

	private static final Logger LOG = LoggerFactory.getLogger(PojoSerializerSnapshotData.class);

	// ---------------------------------------------------------------------------------------------
	//  Factory methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Creates a {@link PojoSerializerSnapshotData} from configuration of a {@link PojoSerializer}.
	 *
	 * <p>This factory method is meant to be used in regular write paths, i.e. when taking a snapshot
	 * of the {@link PojoSerializer}. All registered subclass classes, and non-registered
	 * subclass classes are all present. Some POJO fields may be absent, if the originating
	 * {@link PojoSerializer} was a restored one with already missing fields, and was never replaced
	 * by a new {@link PojoSerializer} (i.e. because the serialized old data was never accessed).
	 */
	static <T> PojoSerializerSnapshotData<T> createFrom(
			Class<T> pojoClass,
			Field[] fields,
			TypeSerializer<?>[] fieldSerializers,
			LinkedHashMap<Class<?>, TypeSerializer<?>> registeredSubclassSerializers,
			Map<Class<?>, TypeSerializer<?>> nonRegisteredSubclassSerializers) {

		final LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots = new LinkedOptionalMap<>(fields.length);

		for (int i = 0; i < fields.length; i++) {
			Field field = fields[i];
			String fieldName = (field == null) ? getDummyNameForMissingField(i) : field.getName();
			fieldSerializerSnapshots.put(fieldName, field, TypeSerializerUtils.snapshotBackwardsCompatible(fieldSerializers[i]));
		}

		LinkedHashMap<Class<?>, TypeSerializerSnapshot<?>> registeredSubclassSerializerSnapshots = new LinkedHashMap<>(registeredSubclassSerializers.size());
		registeredSubclassSerializers.forEach((k, v) -> registeredSubclassSerializerSnapshots.put(k, TypeSerializerUtils.snapshotBackwardsCompatible(v)));

		Map<Class<?>, TypeSerializerSnapshot<?>> nonRegisteredSubclassSerializerSnapshots = new HashMap<>(nonRegisteredSubclassSerializers.size());
		nonRegisteredSubclassSerializers.forEach((k, v) -> nonRegisteredSubclassSerializerSnapshots.put(k, TypeSerializerUtils.snapshotBackwardsCompatible(v)));

		return new PojoSerializerSnapshotData<>(
			pojoClass,
			fieldSerializerSnapshots,
			optionalMapOf(registeredSubclassSerializerSnapshots, Class::getName),
			optionalMapOf(nonRegisteredSubclassSerializerSnapshots, Class::getName));
	}

	/**
	 * Creates a {@link PojoSerializerSnapshotData} from serialized data stream.
	 *
	 * <p>This factory method is meant to be used in regular read paths, i.e. when reading back a snapshot
	 * of the {@link PojoSerializer}. POJO fields, registered subclass classes, and non-registered subclass
	 * classes may no longer be present anymore.
	 */
	static <T> PojoSerializerSnapshotData<T> createFrom(DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		return PojoSerializerSnapshotData.readSnapshotData(in, userCodeClassLoader);
	}

	/**
	 * Creates a {@link PojoSerializerSnapshotData} from existing snapshotted configuration of a {@link PojoSerializer}.
	 */
	static <T> PojoSerializerSnapshotData<T> createFrom(
			Class<T> pojoClass,
			Field[] fields,
			TypeSerializerSnapshot<?>[] existingFieldSerializerSnapshots,
			LinkedHashMap<Class<?>, TypeSerializerSnapshot<?>> existingRegisteredSubclassSerializerSnapshots,
			Map<Class<?>, TypeSerializerSnapshot<?>> existingNonRegisteredSubclassSerializerSnapshots) {

		final LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots = new LinkedOptionalMap<>(fields.length);
		for (int i = 0; i < fields.length; i++) {
			Field field = fields[i];
			String fieldName = (field == null) ? getDummyNameForMissingField(i) : field.getName();
			fieldSerializerSnapshots.put(fieldName, field, existingFieldSerializerSnapshots[i]);
		}

		return new PojoSerializerSnapshotData<>(
			pojoClass,
			fieldSerializerSnapshots,
			optionalMapOf(existingRegisteredSubclassSerializerSnapshots, Class::getName),
			optionalMapOf(existingNonRegisteredSubclassSerializerSnapshots, Class::getName));
	}

	private Class<T> pojoClass;
	private LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots;
	private LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSubclassSerializerSnapshots;
	private LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> nonRegisteredSubclassSerializerSnapshots;

	private PojoSerializerSnapshotData(
			Class<T> typeClass,
			LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots,
			LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSubclassSerializerSnapshots,
			LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> nonRegisteredSubclassSerializerSnapshots) {

		this.pojoClass = checkNotNull(typeClass);
		this.fieldSerializerSnapshots = checkNotNull(fieldSerializerSnapshots);
		this.registeredSubclassSerializerSnapshots = checkNotNull(registeredSubclassSerializerSnapshots);
		this.nonRegisteredSubclassSerializerSnapshots = checkNotNull(nonRegisteredSubclassSerializerSnapshots);
	}

	// ---------------------------------------------------------------------------------------------
	//  Snapshot data read / write methods
	// ---------------------------------------------------------------------------------------------

	void writeSnapshotData(DataOutputView out) throws IOException {
		out.writeUTF(pojoClass.getName());
		writeOptionalMap(out, fieldSerializerSnapshots, PojoFieldUtils::writeField, TypeSerializerSnapshot::writeVersionedSnapshot);
		writeOptionalMap(out, registeredSubclassSerializerSnapshots, NoOpWriter.noopWriter(), TypeSerializerSnapshot::writeVersionedSnapshot);
		writeOptionalMap(out, nonRegisteredSubclassSerializerSnapshots, NoOpWriter.noopWriter(), TypeSerializerSnapshot::writeVersionedSnapshot);
	}

	private static <T> PojoSerializerSnapshotData<T> readSnapshotData(DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		Class<T> pojoClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);

		LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> fieldSerializerSnapshots = readOptionalMap(
			in,
			fieldReader(userCodeClassLoader),
			snapshotReader(userCodeClassLoader));
		LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> registeredSubclassSerializerSnapshots = readOptionalMap(
			in,
			classReader(userCodeClassLoader),
			snapshotReader(userCodeClassLoader));
		LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> nonRegisteredSubclassSerializerSnapshots = readOptionalMap(
			in,
			classReader(userCodeClassLoader),
			snapshotReader(userCodeClassLoader));

		return new PojoSerializerSnapshotData<>(pojoClass, fieldSerializerSnapshots, registeredSubclassSerializerSnapshots, nonRegisteredSubclassSerializerSnapshots);
	}

	// ---------------------------------------------------------------------------------------------
	//  Snapshot data accessors
	// ---------------------------------------------------------------------------------------------

	Class<T> getPojoClass() {
		return pojoClass;
	}

	LinkedOptionalMap<Field, TypeSerializerSnapshot<?>> getFieldSerializerSnapshots() {
		return fieldSerializerSnapshots;
	}

	LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> getRegisteredSubclassSerializerSnapshots() {
		return registeredSubclassSerializerSnapshots;
	}

	LinkedOptionalMap<Class<?>, TypeSerializerSnapshot<?>> getNonRegisteredSubclassSerializerSnapshots() {
		return nonRegisteredSubclassSerializerSnapshots;
	}

	// ---------------------------------------------------------------------------------------------
	//  Utilities
	// ---------------------------------------------------------------------------------------------

	private static String getDummyNameForMissingField(int fieldIndex) {
		return String.format("missing-field-at-%d", fieldIndex);
	}

	private enum NoOpWriter implements BiConsumerWithException<DataOutputView, Object, IOException> {
		INSTANCE;

		@Override
		public void accept(DataOutputView dataOutputView, Object o) {}

		@SuppressWarnings("unchecked")
		static <K> BiConsumerWithException<DataOutputView, K, IOException> noopWriter() {
			return (BiConsumerWithException<DataOutputView, K, IOException>) INSTANCE;
		}
	}

	private static BiFunctionWithException<DataInputView, String, Field, IOException> fieldReader(ClassLoader cl) {
		return (input, fieldName) -> {
			try {
				return PojoFieldUtils.readField(input, cl);
			}
			catch (Throwable t) {
				LOG.warn(String.format("Exception while reading field %s", fieldName), t);
				return null;
			}
		};
	}

	private static BiFunctionWithException<DataInputView, String, TypeSerializerSnapshot<?>, IOException> snapshotReader(ClassLoader cl) {
		return (input, unused) -> {
			try {
				return TypeSerializerSnapshot.readVersionedSnapshot(input, cl);
			}
			catch (Throwable t) {
				LOG.warn("Exception while reading serializer snapshot.", t);
				return null;
			}
		};
	}

	private static BiFunctionWithException<DataInputView, String, Class<?>, IOException> classReader(ClassLoader cl) {
		return (input, className) -> {
			try {
				// input is ignored because we don't write the actual class as value.
				return Class.forName(className, false, cl);
			} catch (Throwable t) {
				LOG.warn(String.format("Exception while reading class %s", className), t);
				return null;
			}
		};
	}
}
