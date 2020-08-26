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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.LegacySerializerSnapshotTransformer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public final class PojoSerializer<T> extends TypeSerializer<T> {

	// Flags for the header
	private static byte IS_NULL = 1;
	private static byte NO_SUBCLASS = 2;
	private static byte IS_SUBCLASS = 4;
	private static byte IS_TAGGED_SUBCLASS = 8;

	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	// PojoSerializer parameters
	// --------------------------------------------------------------------------------------------

	/** The POJO type class. */
	private final Class<T> clazz;

	/**
	 * A cached clazz constructor.
	 * Invoking it using reflection (e.g. clazz.newInstance) has a lot of overhead.
	 */

	private transient MethodHandle cachedConstructor;

	/**
	 * Fields of the POJO and their serializers.
	 *
	 * <p>The fields are kept as a separate transient member, with their serialization
	 * handled with the {@link #readObject(ObjectInputStream)} and {@link #writeObject(ObjectOutputStream)}
	 * methods.
	 */
	private transient Field[] fields;
	private final TypeSerializer<Object>[] fieldSerializers;
	private final int numFields;

	/**
	 * Registered subclasses and their serializers.
	 * Each subclass to their registered class tag is maintained as a separate map ordered by the class tag.
	 */
	private final LinkedHashMap<Class<?>, Integer> registeredClasses;
	private final TypeSerializer<?>[] registeredSerializers;

	/**
	 * Cache of non-registered subclasses to their serializers, created on-the-fly.
	 */
	private transient Map<Class<?>, TypeSerializer<?>> subclassSerializerCache;

	// --------------------------------------------------------------------------------------------

	/**
	 * Configuration of the current execution.
	 */
	private final ExecutionConfig executionConfig;

	private transient ClassLoader cl;

	/**
	 * Constructor to create a new {@link PojoSerializer}.
	 */
	@SuppressWarnings("unchecked")
	public PojoSerializer(
			Class<T> clazz,
			TypeSerializer<?>[] fieldSerializers,
			Field[] fields,
			ExecutionConfig executionConfig) {

		this.clazz = checkNotNull(clazz);
		this.fieldSerializers = (TypeSerializer<Object>[]) checkNotNull(fieldSerializers);
		this.fields = checkNotNull(fields);
		this.numFields = fieldSerializers.length;
		this.executionConfig = checkNotNull(executionConfig);

		for (int i = 0; i < numFields; i++) {
			this.fields[i].setAccessible(true);
		}

		this.cl = Thread.currentThread().getContextClassLoader();

		// We only want those classes that are not our own class and are actually sub-classes.
		LinkedHashSet<Class<?>> registeredSubclasses =
				getRegisteredSubclassesFromExecutionConfig(clazz, executionConfig);

		this.registeredClasses = createRegisteredSubclassTags(registeredSubclasses);
		this.registeredSerializers = createRegisteredSubclassSerializers(registeredSubclasses, executionConfig);

		this.subclassSerializerCache = new HashMap<>();
	}

	/**
	 * Constructor to create a restore serializer or a reconfigured serializer
	 * from a {@link PojoSerializerSnapshot}.
	 */
	PojoSerializer(
			Class<T> clazz,
			Field[] fields,
			TypeSerializer<Object>[] fieldSerializers,
			LinkedHashMap<Class<?>, Integer> registeredClasses,
			TypeSerializer<?>[] registeredSerializers,
			Map<Class<?>, TypeSerializer<?>> subclassSerializerCache,
			ExecutionConfig executionConfig) {

		this.clazz = checkNotNull(clazz);

		this.fields = checkNotNull(fields);
		this.numFields = fields.length;
		this.fieldSerializers = checkNotNull(fieldSerializers);
		this.registeredClasses = checkNotNull(registeredClasses);
		this.registeredSerializers = checkNotNull(registeredSerializers);
		this.subclassSerializerCache = checkNotNull(subclassSerializerCache);
		this.executionConfig = checkNotNull(executionConfig);
		this.cl = Thread.currentThread().getContextClassLoader();
	}
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public PojoSerializer<T> duplicate() {
		boolean stateful = false;
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];

		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
			if (duplicateFieldSerializers[i] != fieldSerializers[i]) {
				// at least one of them is stateful
				stateful = true;
			}
		}

		if (!stateful) {
			// as a small memory optimization, we can share the same object between instances
			duplicateFieldSerializers = fieldSerializers;
		}

		// we must create a new instance, otherwise the subclassSerializerCache can create concurrency problems
		return new PojoSerializer<>(clazz, duplicateFieldSerializers, fields, executionConfig);
	}

	
	@Override
	public T createInstance() {
		if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
			return null;
		}
		try {
			if (cachedConstructor == null) {
				// constructor metahandle is not serializable, so we cannot populate this cache while
				// building the serializer, as the whole serializer needs to be serializable
				cachedConstructor = MethodHandles.lookup()
					.findConstructor(clazz, MethodType.methodType(void.class));
			}
			T t = (T) cachedConstructor.invoke();
			initializeFields(t);
			return t;
		}
		catch (Throwable e) {
			throw new RuntimeException("Cannot instantiate class.", e);
		}
	}

	protected void initializeFields(T t) {
		for (int i = 0; i < numFields; i++) {
			if (fields[i] != null) {
				try {
					fields[i].set(t, fieldSerializers[i].createInstance());
				} catch (IllegalAccessException e) {
					throw new RuntimeException("Cannot initialize fields.", e);
				}
			}
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public T copy(T from) {
		if (from == null) {
			return null;
		}

		Class<?> actualType = from.getClass();
		if (actualType == clazz) {
			T target;
			try {
				target = (T) from.getClass().newInstance();
			}
			catch (Throwable t) {
				throw new RuntimeException("Cannot instantiate class.", t);
			}
			// no subclass
			try {
				for (int i = 0; i < numFields; i++) {
					if (fields[i] != null) {
						Object value = fields[i].get(from);
						if (value != null) {
							Object copy = fieldSerializers[i].copy(value);
							fields[i].set(target, copy);
						} else {
							fields[i].set(target, null);
						}
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields before.");

			}
			return target;
		} else {
			// subclass
			TypeSerializer subclassSerializer = getSubclassSerializer(actualType);
			return (T) subclassSerializer.copy(from);
		}
	}
	
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public T copy(T from, T reuse) {
		if (from == null) {
			return null;
		}

		Class<?> actualType = from.getClass();
		if (reuse == null || actualType != reuse.getClass()) {
			// cannot reuse, do a non-reuse copy
			return copy(from);
		}

		if (actualType == clazz) {
			try {
				for (int i = 0; i < numFields; i++) {
					if (fields[i] != null) {
						Object value = fields[i].get(from);
						if (value != null) {
							Object reuseValue = fields[i].get(reuse);
							Object copy;
							if (reuseValue != null) {
								copy = fieldSerializers[i].copy(value, reuseValue);
							} else {
								copy = fieldSerializers[i].copy(value);
							}
							fields[i].set(reuse, copy);
						} else {
							fields[i].set(reuse, null);
						}
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields before.", e);
			}
		} else {
			TypeSerializer subclassSerializer = getSubclassSerializer(actualType);
			reuse = (T) subclassSerializer.copy(from, reuse);
		}

		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}


	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void serialize(T value, DataOutputView target) throws IOException {
		int flags = 0;
		// handle null values
		if (value == null) {
			flags |= IS_NULL;
			target.writeByte(flags);
			return;
		}

		Integer subclassTag = -1;
		Class<?> actualClass = value.getClass();
		TypeSerializer subclassSerializer = null;
		if (clazz != actualClass) {
			subclassTag = registeredClasses.get(actualClass);
			if (subclassTag != null) {
				flags |= IS_TAGGED_SUBCLASS;
				subclassSerializer = registeredSerializers[subclassTag];
			} else {
				flags |= IS_SUBCLASS;
				subclassSerializer = getSubclassSerializer(actualClass);
			}
		} else {
			flags |= NO_SUBCLASS;
		}

		target.writeByte(flags);

		// if its a registered subclass, write the class tag id, otherwise write the full classname
		if ((flags & IS_SUBCLASS) != 0) {
			target.writeUTF(actualClass.getName());
		} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
			target.writeByte(subclassTag);
		}

		// if its a subclass, use the corresponding subclass serializer,
		// otherwise serialize each field with our field serializers
		if ((flags & NO_SUBCLASS) != 0) {
			try {
				for (int i = 0; i < numFields; i++) {
					Object o = (fields[i] != null) ? fields[i].get(value) : null;
					if (o == null) {
						target.writeBoolean(true); // null field handling
					} else {
						target.writeBoolean(false);
						fieldSerializers[i].serialize(o, target);
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields before.", e);
			}
		} else {
			// subclass
			if (subclassSerializer != null) {
				subclassSerializer.serialize(value, target);
			}
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public T deserialize(DataInputView source) throws IOException {
		int flags = source.readByte();
		if((flags & IS_NULL) != 0) {
			return null;
		}

		T target;

		Class<?> actualSubclass = null;
		TypeSerializer subclassSerializer = null;

		if ((flags & IS_SUBCLASS) != 0) {
			String subclassName = source.readUTF();
			try {
				actualSubclass = Class.forName(subclassName, true, cl);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Cannot instantiate class.", e);
			}
			subclassSerializer = getSubclassSerializer(actualSubclass);
			target = (T) subclassSerializer.createInstance();
			// also initialize fields for which the subclass serializer is not responsible
			initializeFields(target);
		} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {

			int subclassTag = source.readByte();
			subclassSerializer = registeredSerializers[subclassTag];
			target = (T) subclassSerializer.createInstance();
			// also initialize fields for which the subclass serializer is not responsible
			initializeFields(target);
		} else {
			target = createInstance();
		}

		if ((flags & NO_SUBCLASS) != 0) {
			try {
				for (int i = 0; i < numFields; i++) {
					boolean isNull = source.readBoolean();

					if (fields[i] != null) {
						if (isNull) {
							fields[i].set(target, null);
						} else {
							Object field = fieldSerializers[i].deserialize(source);
							fields[i].set(target, field);
						}
					} else if (!isNull) {
						// read and dump a pre-existing field value
						fieldSerializers[i].deserialize(source);
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields before.", e);
			}
		} else {
			if (subclassSerializer != null) {
				target = (T) subclassSerializer.deserialize(target, source);
			}
		}
		return target;
	}
	
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public T deserialize(T reuse, DataInputView source) throws IOException {

		// handle null values
		int flags = source.readByte();
		if((flags & IS_NULL) != 0) {
			return null;
		}

		Class<?> subclass = null;
		TypeSerializer subclassSerializer = null;
		if ((flags & IS_SUBCLASS) != 0) {
			String subclassName = source.readUTF();
			try {
				subclass = Class.forName(subclassName, true, cl);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Cannot instantiate class.", e);
			}
			subclassSerializer = getSubclassSerializer(subclass);

			if (reuse == null || subclass != reuse.getClass()) {
				// cannot reuse
				reuse = (T) subclassSerializer.createInstance();
				// also initialize fields for which the subclass serializer is not responsible
				initializeFields(reuse);
			}
		} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
			int subclassTag = source.readByte();
			subclassSerializer = registeredSerializers[subclassTag];

			if (reuse == null || ((PojoSerializer)subclassSerializer).clazz != reuse.getClass()) {
				// cannot reuse
				reuse = (T) subclassSerializer.createInstance();
				// also initialize fields for which the subclass serializer is not responsible
				initializeFields(reuse);
			}
		} else {
			if (reuse == null || clazz != reuse.getClass()) {
				reuse = createInstance();
			}
		}

		if ((flags & NO_SUBCLASS) != 0) {
			try {
				for (int i = 0; i < numFields; i++) {
					boolean isNull = source.readBoolean();

					if (fields[i] != null) {
						if (isNull) {
							fields[i].set(reuse, null);
						} else {
							Object field;

							Object reuseField = fields[i].get(reuse);
							if (reuseField != null) {
								field = fieldSerializers[i].deserialize(reuseField, source);
							} else {
								field = fieldSerializers[i].deserialize(source);
							}

							fields[i].set(reuse, field);
						}
					} else if (!isNull) {
						// read and dump a pre-existing field value
						fieldSerializers[i].deserialize(source);
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields before.", e);
			}
		} else {
			if (subclassSerializer != null) {
				reuse = (T) subclassSerializer.deserialize(reuse, source);
			}
		}

		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// copy the flags
		int flags = source.readByte();
		target.writeByte(flags);

		if ((flags & IS_NULL) != 0) {
			// is a null value, nothing further to copy
			return;
		}

		TypeSerializer<?> subclassSerializer = null;
		if ((flags & IS_SUBCLASS) != 0) {
			String className = source.readUTF();
			target.writeUTF(className);
			try {
				Class<?> subclass = Class.forName(className, true, Thread.currentThread()
						.getContextClassLoader());
				subclassSerializer = getSubclassSerializer(subclass);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Cannot instantiate class.", e);
			}
		} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
			int subclassTag = source.readByte();
			target.writeByte(subclassTag);
			subclassSerializer = registeredSerializers[subclassTag];
		}

		if ((flags & NO_SUBCLASS) != 0) {
			for (int i = 0; i < numFields; i++) {
				boolean isNull = source.readBoolean();
				target.writeBoolean(isNull);
				if (!isNull) {
					fieldSerializers[i].copy(source, target);
				}
			}
		} else {
			if (subclassSerializer != null) {
				subclassSerializer.copy(source, target);
			}
		}
	}
	
	@Override
	public int hashCode() {
		return 31 * (31 * Arrays.hashCode(fieldSerializers) + Arrays.hashCode(registeredSerializers)) +
			Objects.hash(clazz, numFields, registeredClasses);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PojoSerializer) {
			PojoSerializer<?> other = (PojoSerializer<?>) obj;

			return clazz == other.clazz &&
				Arrays.equals(fieldSerializers, other.fieldSerializers) &&
				Arrays.equals(registeredSerializers, other.registeredSerializers) &&
				numFields == other.numFields &&
				registeredClasses.equals(other.registeredClasses);
		} else {
			return false;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public PojoSerializerSnapshot<T> snapshotConfiguration() {
		return buildSnapshot(
				clazz,
				registeredClasses,
				registeredSerializers,
				fields,
				fieldSerializers,
				subclassSerializerCache);
	}

	/**
	 * @deprecated This snapshot class is no longer being used.
	 *             It has been fully replaced by {@link PojoSerializerSnapshot}.
	 */
	@Deprecated
	public static final class PojoSerializerConfigSnapshot<T> extends GenericTypeSerializerConfigSnapshot<T> {

		private static final int VERSION = 1;

		/**
		 * Ordered map of POJO fields to their corresponding serializers and its configuration snapshots.
		 *
		 * <p>Ordering of the fields is kept so that new Pojo serializers for previous data
		 * may reorder the fields in case they are different. The order of the fields need to
		 * stay the same for binary compatibility, as the field order is part of the serialization format.
		 */
		private LinkedHashMap<String, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> fieldToSerializerConfigSnapshot;

		/**
		 * Ordered map of registered subclasses to their corresponding serializers and its configuration snapshots.
		 *
		 * <p>Ordering of the registered subclasses is kept so that new Pojo serializers for previous data
		 * may retain the same class tag used for registered subclasses. Newly registered subclasses that
		 * weren't present before should be appended with the next available class tag.
		 */
		private LinkedHashMap<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> registeredSubclassesToSerializerConfigSnapshots;

		/**
		 * Previously cached non-registered subclass serializers and its configuration snapshots.
		 *
		 * <p>This is kept so that new Pojo serializers may eagerly repopulate their
		 * cache with reconfigured subclass serializers.
		 */
		private HashMap<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> nonRegisteredSubclassesToSerializerConfigSnapshots;

		private boolean ignoreTypeSerializerSerialization;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public PojoSerializerConfigSnapshot() {}

		public PojoSerializerConfigSnapshot(
				Class<T> pojoType,
				LinkedHashMap<String, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> fieldToSerializerConfigSnapshot,
				LinkedHashMap<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> registeredSubclassesToSerializerConfigSnapshots,
				HashMap<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> nonRegisteredSubclassesToSerializerConfigSnapshots) {

			this(
				pojoType,
				fieldToSerializerConfigSnapshot,
				registeredSubclassesToSerializerConfigSnapshots,
				nonRegisteredSubclassesToSerializerConfigSnapshots,
				false);
		}

		public PojoSerializerConfigSnapshot(
				Class<T> pojoType,
				LinkedHashMap<String, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> fieldToSerializerConfigSnapshot,
				LinkedHashMap<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> registeredSubclassesToSerializerConfigSnapshots,
				HashMap<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> nonRegisteredSubclassesToSerializerConfigSnapshots,
				boolean ignoreTypeSerializerSerialization) {

			super(pojoType);

			this.fieldToSerializerConfigSnapshot =
					Preconditions.checkNotNull(fieldToSerializerConfigSnapshot);
			this.registeredSubclassesToSerializerConfigSnapshots =
					Preconditions.checkNotNull(registeredSubclassesToSerializerConfigSnapshots);
			this.nonRegisteredSubclassesToSerializerConfigSnapshots =
					Preconditions.checkNotNull(nonRegisteredSubclassesToSerializerConfigSnapshots);

			this.ignoreTypeSerializerSerialization = ignoreTypeSerializerSerialization;
		}

		/**
		 * This legacy snapshot delegates compatibility checks to the {@link PojoSerializerSnapshot}.
		 */
		@Override
		public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
			LinkedHashMap<String, TypeSerializerSnapshot<?>> legacyFieldSerializerSnapshots =
				preprocessLegacySerializerSnapshotTuples(fieldToSerializerConfigSnapshot);

			int numFields = legacyFieldSerializerSnapshots.size();
			ArrayList<Field> fields = new ArrayList<>(numFields);
			ArrayList<TypeSerializerSnapshot<?>> fieldSerializerSnapshots = new ArrayList<>(numFields);
			legacyFieldSerializerSnapshots.forEach((fieldName, fieldSerializerSnapshot) -> {
				fields.add(PojoFieldUtils.getField(fieldName, getTypeClass()));
				fieldSerializerSnapshots.add(fieldSerializerSnapshot);
			});

			PojoSerializerSnapshot<T> newSnapshot = new PojoSerializerSnapshot<>(
				getTypeClass(),
				fields.toArray(new Field[numFields]),
				fieldSerializerSnapshots.toArray(new TypeSerializerSnapshot[numFields]),
				preprocessLegacySerializerSnapshotTuples(registeredSubclassesToSerializerConfigSnapshots),
				preprocessLegacySerializerSnapshotTuples(nonRegisteredSubclassesToSerializerConfigSnapshots));

			return newSnapshot.resolveSchemaCompatibility(newSerializer);
		}

		@SuppressWarnings("unchecked")
		private static <K> LinkedHashMap<K, TypeSerializerSnapshot<?>> preprocessLegacySerializerSnapshotTuples(Map<K, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> originalMap) {
			LinkedHashMap<K, TypeSerializerSnapshot<?>> converted = new LinkedHashMap<>(originalMap.size());

			originalMap.forEach((key, serializerSnapshotTuple) -> {
				TypeSerializer<?> serializer = serializerSnapshotTuple.f0;
				TypeSerializerSnapshot<?> snapshot = serializerSnapshotTuple.f1;

				if (snapshot instanceof TypeSerializerConfigSnapshot) {
					((TypeSerializerConfigSnapshot) snapshot).setPriorSerializer(serializer);
				}

				if (serializer instanceof LegacySerializerSnapshotTransformer) {
					snapshot = ((LegacySerializerSnapshotTransformer) serializer).transformLegacySerializerSnapshot(snapshot);
				}

				converted.put(key, snapshot);
			});

			return converted;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			try (
				ByteArrayOutputStreamWithPos outWithPos = new ByteArrayOutputStreamWithPos();
				DataOutputViewStreamWrapper outViewWrapper = new DataOutputViewStreamWrapper(outWithPos)) {

				// --- write fields and their serializers, in order

				out.writeInt(fieldToSerializerConfigSnapshot.size());
				for (Map.Entry<String, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> entry
						: fieldToSerializerConfigSnapshot.entrySet()) {

					outViewWrapper.writeUTF(entry.getKey());

					out.writeInt(outWithPos.getPosition());
					if (!ignoreTypeSerializerSerialization) {
						TypeSerializerSerializationUtil.writeSerializer(outViewWrapper, entry.getValue().f0);
					}

					out.writeInt(outWithPos.getPosition());
					TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
						outViewWrapper, (TypeSerializerSnapshot) entry.getValue().f1, entry.getValue().f0);
				}

				// --- write registered subclasses and their serializers, in registration order

				out.writeInt(registeredSubclassesToSerializerConfigSnapshots.size());
				for (Map.Entry<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> entry
						: registeredSubclassesToSerializerConfigSnapshots.entrySet()) {

					outViewWrapper.writeUTF(entry.getKey().getName());

					out.writeInt(outWithPos.getPosition());
					if (!ignoreTypeSerializerSerialization) {
						TypeSerializerSerializationUtil.writeSerializer(outViewWrapper, entry.getValue().f0);
					}

					out.writeInt(outWithPos.getPosition());
					TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
						outViewWrapper, (TypeSerializerSnapshot) entry.getValue().f1, entry.getValue().f0);
				}

				// --- write snapshot of non-registered subclass serializer cache

				out.writeInt(nonRegisteredSubclassesToSerializerConfigSnapshots.size());
				for (Map.Entry<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> entry
						: nonRegisteredSubclassesToSerializerConfigSnapshots.entrySet()) {

					outViewWrapper.writeUTF(entry.getKey().getName());

					out.writeInt(outWithPos.getPosition());
					if (!ignoreTypeSerializerSerialization) {
						TypeSerializerSerializationUtil.writeSerializer(outViewWrapper, entry.getValue().f0);
					}

					out.writeInt(outWithPos.getPosition());
					TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
						outViewWrapper, (TypeSerializerSnapshot) entry.getValue().f1, entry.getValue().f0);
				}

				out.writeInt(outWithPos.getPosition());
				out.write(outWithPos.getBuf(), 0 , outWithPos.getPosition());
			}
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			int numFields = in.readInt();
			int[] fieldSerializerOffsets = new int[numFields * 2];
			for (int i = 0; i < numFields; i++) {
				fieldSerializerOffsets[i * 2] = in.readInt();
				fieldSerializerOffsets[i * 2 + 1] = in.readInt();
			}


			int numRegisteredSubclasses = in.readInt();
			int[] registeredSerializerOffsets = new int[numRegisteredSubclasses * 2];
			for (int i = 0; i < numRegisteredSubclasses; i++) {
				registeredSerializerOffsets[i * 2] = in.readInt();
				registeredSerializerOffsets[i * 2 + 1] = in.readInt();
			}

			int numCachedSubclassSerializers = in.readInt();
			int[] cachedSerializerOffsets = new int[numCachedSubclassSerializers * 2];
			for (int i = 0; i < numCachedSubclassSerializers; i++) {
				cachedSerializerOffsets[i * 2] = in.readInt();
				cachedSerializerOffsets[i * 2 + 1] = in.readInt();
			}

			int totalBytes = in.readInt();
			byte[] buffer = new byte[totalBytes];
			in.readFully(buffer);

			try (
				ByteArrayInputStreamWithPos inWithPos = new ByteArrayInputStreamWithPos(buffer);
				DataInputViewStreamWrapper inViewWrapper = new DataInputViewStreamWrapper(inWithPos)) {

				// --- read fields and their serializers, in order

				this.fieldToSerializerConfigSnapshot = new LinkedHashMap<>(numFields);
				String fieldName;
				TypeSerializer<?> fieldSerializer;
				TypeSerializerSnapshot fieldSerializerConfigSnapshot;
				for (int i = 0; i < numFields; i++) {
					fieldName = inViewWrapper.readUTF();

					inWithPos.setPosition(fieldSerializerOffsets[i * 2]);
					fieldSerializer = TypeSerializerSerializationUtil.tryReadSerializer(inViewWrapper, getUserCodeClassLoader(), true);

					inWithPos.setPosition(fieldSerializerOffsets[i * 2 + 1]);
					fieldSerializerConfigSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
						inViewWrapper, getUserCodeClassLoader(), fieldSerializer);

					fieldToSerializerConfigSnapshot.put(
						fieldName,
						new Tuple2<>(fieldSerializer, fieldSerializerConfigSnapshot));
				}

				// --- read registered subclasses and their serializers, in registration order

				this.registeredSubclassesToSerializerConfigSnapshots = new LinkedHashMap<>(numRegisteredSubclasses);
				String registeredSubclassname;
				Class<?> registeredSubclass;
				TypeSerializer<?> registeredSubclassSerializer;
				TypeSerializerSnapshot registeredSubclassSerializerConfigSnapshot;
				for (int i = 0; i < numRegisteredSubclasses; i++) {
					registeredSubclassname = inViewWrapper.readUTF();
					try {
						registeredSubclass = Class.forName(registeredSubclassname, true, getUserCodeClassLoader());
					} catch (ClassNotFoundException e) {
						throw new IOException("Cannot find requested class " + registeredSubclassname + " in classpath.", e);
					}

					inWithPos.setPosition(registeredSerializerOffsets[i * 2]);
					registeredSubclassSerializer = TypeSerializerSerializationUtil.tryReadSerializer(inViewWrapper, getUserCodeClassLoader(), true);

					inWithPos.setPosition(registeredSerializerOffsets[i * 2 + 1]);
					registeredSubclassSerializerConfigSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
						inViewWrapper, getUserCodeClassLoader(), registeredSubclassSerializer);

					this.registeredSubclassesToSerializerConfigSnapshots.put(
						registeredSubclass,
						new Tuple2<>(registeredSubclassSerializer, registeredSubclassSerializerConfigSnapshot));
				}

				// --- read snapshot of non-registered subclass serializer cache

				this.nonRegisteredSubclassesToSerializerConfigSnapshots = new HashMap<>(numCachedSubclassSerializers);
				String cachedSubclassname;
				Class<?> cachedSubclass;
				TypeSerializer<?> cachedSubclassSerializer;
				TypeSerializerSnapshot cachedSubclassSerializerConfigSnapshot;
				for (int i = 0; i < numCachedSubclassSerializers; i++) {
					cachedSubclassname = inViewWrapper.readUTF();
					try {
						cachedSubclass = Class.forName(cachedSubclassname, true, getUserCodeClassLoader());
					} catch (ClassNotFoundException e) {
						throw new IOException("Cannot find requested class " + cachedSubclassname + " in classpath.", e);
					}

					inWithPos.setPosition(cachedSerializerOffsets[i * 2]);
					cachedSubclassSerializer = TypeSerializerSerializationUtil.tryReadSerializer(inViewWrapper, getUserCodeClassLoader(), true);

					inWithPos.setPosition(cachedSerializerOffsets[i * 2 + 1]);
					cachedSubclassSerializerConfigSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
						inViewWrapper, getUserCodeClassLoader(), cachedSubclassSerializer);

					this.nonRegisteredSubclassesToSerializerConfigSnapshots.put(
						cachedSubclass,
						new Tuple2<>(cachedSubclassSerializer, cachedSubclassSerializerConfigSnapshot));
				}
			}
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		public LinkedHashMap<String, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> getFieldToSerializerConfigSnapshot() {
			return fieldToSerializerConfigSnapshot;
		}

		public LinkedHashMap<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> getRegisteredSubclassesToSerializerConfigSnapshots() {
			return registeredSubclassesToSerializerConfigSnapshots;
		}

		public HashMap<Class<?>, Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> getNonRegisteredSubclassesToSerializerConfigSnapshots() {
			return nonRegisteredSubclassesToSerializerConfigSnapshots;
		}

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj)
					&& (obj instanceof PojoSerializerConfigSnapshot)
					&& fieldToSerializerConfigSnapshot.equals(((PojoSerializerConfigSnapshot) obj).getFieldToSerializerConfigSnapshot())
					&& registeredSubclassesToSerializerConfigSnapshots.equals(((PojoSerializerConfigSnapshot) obj).getRegisteredSubclassesToSerializerConfigSnapshots())
					&& nonRegisteredSubclassesToSerializerConfigSnapshots.equals(((PojoSerializerConfigSnapshot) obj).nonRegisteredSubclassesToSerializerConfigSnapshots);
		}

		@Override
		public int hashCode() {
			return super.hashCode()
				+ Objects.hash(
					fieldToSerializerConfigSnapshot,
					registeredSubclassesToSerializerConfigSnapshots,
					nonRegisteredSubclassesToSerializerConfigSnapshots);
		}
	}

	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		out.writeInt(fields.length);
		for (Field field: fields) {
			FieldSerializer.serializeField(field, out);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		int numFields = in.readInt();
		fields = new Field[numFields];
		for (int i = 0; i < numFields; i++) {
			// the deserialized Field may be null if the field no longer exists in the POJO;
			// in this case, when de-/serializing and copying instances using this serializer
			// instance, the missing fields will simply be skipped
			fields[i] = FieldSerializer.deserializeField(in);
		}

		cl = Thread.currentThread().getContextClassLoader();
		subclassSerializerCache = new HashMap<Class<?>, TypeSerializer<?>>();
	}

	// --------------------------------------------------------------------------------------------
	// Configuration access
	// --------------------------------------------------------------------------------------------

	Class<T> getPojoClass() {
		return clazz;
	}

	Field[] getFields() {
		return fields;
	}

	TypeSerializer<?>[] getFieldSerializers() {
		return fieldSerializers;
	}

	TypeSerializer<?> getFieldSerializer(Field targetField) {
		int fieldIndex = findField(targetField.getName());
		if (fieldIndex == -1) {
			return null;
		}
		return fieldSerializers[fieldIndex];
	}

	ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	LinkedHashMap<Class<?>, Integer> getRegisteredClasses() {
		return registeredClasses;
	}

	TypeSerializer<?>[] getRegisteredSerializers() {
		return registeredSerializers;
	}

	LinkedHashMap<Class<?>, TypeSerializer<?>> getBundledSubclassSerializerRegistry() {
		final LinkedHashMap<Class<?>, TypeSerializer<?>> result = new LinkedHashMap<>(registeredClasses.size());
		registeredClasses.forEach((registeredClass, id) -> result.put(registeredClass, registeredSerializers[id]));
		return result;
	}

	Map<Class<?>, TypeSerializer<?>> getSubclassSerializerCache() {
		return subclassSerializerCache;
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Extracts the subclasses of the base POJO class registered in the execution config.
	 */
	private static LinkedHashSet<Class<?>> getRegisteredSubclassesFromExecutionConfig(
			Class<?> basePojoClass,
			ExecutionConfig executionConfig) {

		LinkedHashSet<Class<?>> subclassesInRegistrationOrder = new LinkedHashSet<>(executionConfig.getRegisteredPojoTypes().size());
		for (Class<?> registeredClass : executionConfig.getRegisteredPojoTypes()) {
			if (registeredClass.equals(basePojoClass)) {
				continue;
			}
			if (!basePojoClass.isAssignableFrom(registeredClass)) {
				continue;
			}
			subclassesInRegistrationOrder.add(registeredClass);
		}

		return subclassesInRegistrationOrder;
	}

	/**
	 * Builds map of registered subclasses to their class tags.
	 * Class tags will be integers starting from 0, assigned incrementally with the order of provided subclasses.
	 */
	private static LinkedHashMap<Class<?>, Integer> createRegisteredSubclassTags(LinkedHashSet<Class<?>> registeredSubclasses) {
		final LinkedHashMap<Class<?>, Integer> classToTag = new LinkedHashMap<>();

		int id = 0;
		for (Class<?> registeredClass : registeredSubclasses) {
			classToTag.put(registeredClass, id);
			id ++;
		}

		return classToTag;
	}

	/**
	 * Creates an array of serializers for provided list of registered subclasses.
	 * Order of returned serializers will correspond to order of provided subclasses.
	 */
	private static TypeSerializer<?>[] createRegisteredSubclassSerializers(
			LinkedHashSet<Class<?>> registeredSubclasses,
			ExecutionConfig executionConfig) {

		final TypeSerializer<?>[] subclassSerializers = new TypeSerializer[registeredSubclasses.size()];

		int i = 0;
		for (Class<?> registeredClass : registeredSubclasses) {
			subclassSerializers[i] = TypeExtractor.createTypeInfo(registeredClass).createSerializer(executionConfig);
			i++;
		}

		return subclassSerializers;
	}

	/**
	 * Fetches cached serializer for a non-registered subclass;
	 * also creates the serializer if it doesn't exist yet.
	 *
	 * This method is also exposed to package-private access
	 * for testing purposes.
	 */
	TypeSerializer<?> getSubclassSerializer(Class<?> subclass) {
		TypeSerializer<?> result = subclassSerializerCache.get(subclass);
		if (result == null) {
			result = createSubclassSerializer(subclass);
			subclassSerializerCache.put(subclass, result);
		}
		return result;
	}

	private TypeSerializer<?> createSubclassSerializer(Class<?> subclass) {
		TypeSerializer<?> serializer = TypeExtractor.createTypeInfo(subclass).createSerializer(executionConfig);

		if (serializer instanceof PojoSerializer) {
			PojoSerializer<?> subclassSerializer = (PojoSerializer<?>) serializer;
			subclassSerializer.copyBaseFieldOrder(this);
		}

		return serializer;
	}

	/**
	 * Finds and returns the order (0-based) of a POJO field.
	 * Returns -1 if the field does not exist for this POJO.
	 */
	private int findField(String fieldName) {
		int foundIndex = 0;
		for (Field field : fields) {
			if (field != null && fieldName.equals(field.getName())) {
				return foundIndex;
			}

			foundIndex++;
		}

		return -1;
	}

	private void copyBaseFieldOrder(PojoSerializer<?> baseSerializer) {
		// do nothing for now, but in the future, adapt subclass serializer to have same
		// ordering as base class serializer so that binary comparison on base class fields
		// can work
	}

	/**
	 * Build and return a snapshot of the serializer's parameters and currently cached serializers.
	 */
	private static <T> PojoSerializerSnapshot<T> buildSnapshot(
			Class<T> pojoType,
			LinkedHashMap<Class<?>, Integer> registeredSubclassesToTags,
			TypeSerializer<?>[] registeredSubclassSerializers,
			Field[] fields,
			TypeSerializer<?>[] fieldSerializers,
			Map<Class<?>, TypeSerializer<?>> nonRegisteredSubclassSerializerCache) {

		final LinkedHashMap<Class<?>, TypeSerializer<?>> subclassRegistry = new LinkedHashMap<>(registeredSubclassesToTags.size());

		for (Map.Entry<Class<?>, Integer> entry : registeredSubclassesToTags.entrySet()) {
			subclassRegistry.put(entry.getKey(), registeredSubclassSerializers[entry.getValue()]);
		}

		return new PojoSerializerSnapshot<>(
				pojoType,
				fields,
				fieldSerializers,
				subclassRegistry,
				nonRegisteredSubclassSerializerCache);
	}

}
