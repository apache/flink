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
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerUtil;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

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
	 * Fields of the POJO and their serializers.
	 *
	 * <p>The fields are kept as a separate transient member, with their serialization
	 * handled with the {@link #readObject(ObjectInputStream)} and {@link #writeObject(ObjectOutputStream)}
	 * methods.
	 *
	 * <p>These may be reconfigured in {@link #ensureCompatibility(TypeSerializerConfigSnapshot)}.
	 */
	private transient Field[] fields;
	private TypeSerializer<Object>[] fieldSerializers;
	private final int numFields;

	/**
	 * Registered subclasses and their serializers.
	 * Each subclass to their registered class tag is maintained as a separate map ordered by the class tag.
	 *
	 * <p>These may be reconfigured in {@link #ensureCompatibility(TypeSerializerConfigSnapshot)}.
	 */
	private LinkedHashMap<Class<?>, Integer> registeredClasses;
	private TypeSerializer<?>[] registeredSerializers;

	/**
	 * Cache of non-registered subclasses to their serializers, created on-the-fly.
	 *
	 * <p>This cache is persisted and will be repopulated with reconfigured serializers
	 * in {@link #ensureCompatibility(TypeSerializerConfigSnapshot)}.
	 */
	private transient HashMap<Class<?>, TypeSerializer<?>> subclassSerializerCache;

	// --------------------------------------------------------------------------------------------

	/**
	 * Configuration of the current execution.
	 *
	 * <p>Nested serializers created using this will have the most up-to-date configuration,
	 * and can be resolved for backwards compatibility with previous configuration
	 * snapshots in {@link #ensureCompatibility(TypeSerializerConfigSnapshot)}.
	 */
	private final ExecutionConfig executionConfig;

	private transient ClassLoader cl;

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

		cl = Thread.currentThread().getContextClassLoader();

		// We only want those classes that are not our own class and are actually sub-classes.
		LinkedHashSet<Class<?>> registeredSubclasses =
				getRegisteredSubclassesFromExecutionConfig(clazz, executionConfig);

		this.registeredClasses = createRegisteredSubclassTags(registeredSubclasses);
		this.registeredSerializers = createRegisteredSubclassSerializers(registeredSubclasses, executionConfig);

		this.subclassSerializerCache = new HashMap<>();
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

		if (stateful) {
			return new PojoSerializer<T>(clazz, duplicateFieldSerializers, fields, executionConfig);
		} else {
			return this;
		}
	}

	
	@Override
	public T createInstance() {
		if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
			return null;
		}
		try {
			T t = clazz.newInstance();
			initializeFields(t);
			return t;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate class.", e);
		}
	}

	protected void initializeFields(T t) {
		for (int i = 0; i < numFields; i++) {
			try {
				fields[i].set(t, fieldSerializers[i].createInstance());
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Cannot initialize fields.", e);
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
					Object value = fields[i].get(from);
					if (value != null) {
						Object copy = fieldSerializers[i].copy(value);
						fields[i].set(target, copy);
					}
					else {
						fields[i].set(target, null);
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
					Object value = fields[i].get(from);
					if (value != null) {
						Object reuseValue = fields[i].get(reuse);
						Object copy;
						if(reuseValue != null) {
							copy = fieldSerializers[i].copy(value, reuseValue);
						}
						else {
							copy = fieldSerializers[i].copy(value);
						}
						fields[i].set(reuse, copy);
					}
					else {
						fields[i].set(reuse, null);
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
					Object o = fields[i].get(value);
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
					if (isNull) {
						fields[i].set(target, null);
					} else {
						Object field = fieldSerializers[i].deserialize(source);
						fields[i].set(target, field);
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
					if (isNull) {
						fields[i].set(reuse, null);
					} else {
						Object field;

						Object reuseField = fields[i].get(reuse);
						if(reuseField != null) {
							field = fieldSerializers[i].deserialize(reuseField, source);
						}
						else {
							field = fieldSerializers[i].deserialize(source);
						}

						fields[i].set(reuse, field);
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

			return other.canEqual(this) &&
				clazz == other.clazz &&
				Arrays.equals(fieldSerializers, other.fieldSerializers) &&
				Arrays.equals(registeredSerializers, other.registeredSerializers) &&
				numFields == other.numFields &&
				registeredClasses.equals(other.registeredClasses);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof PojoSerializer;
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public PojoSerializerConfigSnapshot<T> snapshotConfiguration() {
		return buildConfigSnapshot(
				clazz,
				registeredClasses,
				registeredSerializers,
				fields,
				fieldSerializers,
				subclassSerializerCache);
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof PojoSerializerConfigSnapshot) {
			final PojoSerializerConfigSnapshot<T> config = (PojoSerializerConfigSnapshot<T>) configSnapshot;

			if (clazz.equals(config.getTypeClass())) {
				if (this.numFields == config.getFieldToSerializerConfigSnapshot().size()) {

					CompatibilityResult<?> compatResult;

					// ----------- check field order and compatibility of field serializers -----------

					// reordered fields and their serializers;
					// this won't be applied to this serializer until all compatibility checks have been completed
					final Field[] reorderedFields = new Field[this.numFields];
					final TypeSerializer<Object>[] reorderedFieldSerializers =
						(TypeSerializer<Object>[]) new TypeSerializer<?>[this.numFields];

					int i = 0;
					for (Map.Entry<Field, TypeSerializerConfigSnapshot> fieldToConfigSnapshotEntry
							: config.getFieldToSerializerConfigSnapshot().entrySet()) {

						int fieldIndex = findField(fieldToConfigSnapshotEntry.getKey());
						if (fieldIndex != -1) {
							reorderedFields[i] = fieldToConfigSnapshotEntry.getKey();

							compatResult = fieldSerializers[fieldIndex].ensureCompatibility(fieldToConfigSnapshotEntry.getValue());
							if (compatResult.requiresMigration()) {
								return CompatibilityResult.requiresMigration(null);
							} else {
								reorderedFieldSerializers[i] = fieldSerializers[fieldIndex];
							}
						} else {
							return CompatibilityResult.requiresMigration(null);
						}

						i++;
					}

					// ---- check subclass registration order and compatibility of registered serializers ----

					// reordered subclass registrations and their serializers;
					// this won't be applied to this serializer until all compatibility checks have been completed
					final LinkedHashMap<Class<?>, Integer> reorderedRegisteredSubclassesToClasstags;
					final TypeSerializer<?>[] reorderedRegisteredSubclassSerializers;

					final LinkedHashMap<Class<?>, TypeSerializerConfigSnapshot> previousRegistrations =
						config.getRegisteredSubclassesToSerializerConfigSnapshots();

					// the reconfigured list of registered subclasses will be the previous registered
					// subclasses in the original order with new subclasses appended at the end
					LinkedHashSet<Class<?>> reorderedRegisteredSubclasses = new LinkedHashSet<>();
					reorderedRegisteredSubclasses.addAll(previousRegistrations.keySet());
					reorderedRegisteredSubclasses.addAll(
						getRegisteredSubclassesFromExecutionConfig(clazz, executionConfig));

					// re-establish the registered class tags and serializers
					reorderedRegisteredSubclassesToClasstags = createRegisteredSubclassTags(reorderedRegisteredSubclasses);
					reorderedRegisteredSubclassSerializers = createRegisteredSubclassSerializers(
						reorderedRegisteredSubclasses, executionConfig);

					i = 0;
					for (TypeSerializerConfigSnapshot previousRegisteredSerializerConfig : previousRegistrations.values()) {
						// check compatibility of subclass serializer
						compatResult = reorderedRegisteredSubclassSerializers[i].ensureCompatibility(previousRegisteredSerializerConfig);
						if (compatResult.requiresMigration()) {
							return CompatibilityResult.requiresMigration(null);
						}

						i++;
					}

					// ------------------ ensure compatibility of non-registered subclass serializers ------------------

					// the rebuilt cache for non-registered subclass serializers;
					// this won't be applied to this serializer until all compatibility checks have been completed
					HashMap<Class<?>, TypeSerializer<?>> rebuiltCache = new HashMap<>();

					for (Map.Entry<Class<?>, TypeSerializerConfigSnapshot> previousCachedEntry
							: config.getNonRegisteredSubclassesToSerializerConfigSnapshots().entrySet()) {

						TypeSerializer<?> cachedSerializer = createSubclassSerializer(previousCachedEntry.getKey());

						// check compatibility of cached subclass serializer
						compatResult = cachedSerializer.ensureCompatibility(previousCachedEntry.getValue());
						if (compatResult.requiresMigration()) {
							return CompatibilityResult.requiresMigration(null);
						} else {
							rebuiltCache.put(previousCachedEntry.getKey(), cachedSerializer);
						}
					}

					// completed compatibility checks; up to this point, we can just reconfigure
					// the serializer so that it is compatible and migration is not required

					this.fields = reorderedFields;
					this.fieldSerializers = reorderedFieldSerializers;

					this.registeredClasses = reorderedRegisteredSubclassesToClasstags;
					this.registeredSerializers = reorderedRegisteredSubclassSerializers;

					this.subclassSerializerCache = rebuiltCache;

					return CompatibilityResult.compatible();
				}
			}
		}

		return CompatibilityResult.requiresMigration(null);
	}

	public static final class PojoSerializerConfigSnapshot<T> extends GenericTypeSerializerConfigSnapshot<T> {

		private static final int VERSION = 1;

		/**
		 * Ordered map of POJO fields to the configuration snapshots of their corresponding serializers.
		 *
		 * <p>Ordering of the fields is kept so that new Pojo serializers for previous data
		 * may reorder the fields in case they are different. The order of the fields need to
		 * stay the same for binary compatibility, as the field order is part of the serialization format.
		 */
		private LinkedHashMap<Field, TypeSerializerConfigSnapshot> fieldToSerializerConfigSnapshot;

		/**
		 * Ordered map of registered subclasses to the configuration snapshots of their corresponding serializers.
		 *
		 * <p>Ordering of the registered subclasses is kept so that new Pojo serializers for previous data
		 * may retain the same class tag used for registered subclasses. Newly registered subclasses that
		 * weren't present before should be appended with the next available class tag.
		 */
		private LinkedHashMap<Class<?>, TypeSerializerConfigSnapshot> registeredSubclassesToSerializerConfigSnapshots;

		/**
		 * Configuration snapshots of previously cached non-registered subclass serializers.
		 *
		 * <p>This is kept so that new Pojo serializers may eagerly repopulate their
		 * cache with reconfigured subclass serializers.
		 */
		private HashMap<Class<?>, TypeSerializerConfigSnapshot> nonRegisteredSubclassesToSerializerConfigSnapshots;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public PojoSerializerConfigSnapshot() {}

		public PojoSerializerConfigSnapshot(
				Class<T> pojoType,
				LinkedHashMap<Field, TypeSerializerConfigSnapshot> fieldToSerializerConfigSnapshot,
				LinkedHashMap<Class<?>, TypeSerializerConfigSnapshot> registeredSubclassesToSerializerConfigSnapshots,
				HashMap<Class<?>, TypeSerializerConfigSnapshot> nonRegisteredSubclassesToSerializerConfigSnapshots) {

			super(pojoType);

			this.fieldToSerializerConfigSnapshot =
					Preconditions.checkNotNull(fieldToSerializerConfigSnapshot);
			this.registeredSubclassesToSerializerConfigSnapshots =
					Preconditions.checkNotNull(registeredSubclassesToSerializerConfigSnapshots);
			this.nonRegisteredSubclassesToSerializerConfigSnapshots =
					Preconditions.checkNotNull(nonRegisteredSubclassesToSerializerConfigSnapshots);
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			// --- write fields and their serializers, in order

			out.writeInt(fieldToSerializerConfigSnapshot.size());
			for (Map.Entry<Field, TypeSerializerConfigSnapshot> entry
				: fieldToSerializerConfigSnapshot.entrySet()) {
				out.writeUTF(entry.getKey().getName());
				TypeSerializerUtil.writeSerializerConfigSnapshot(out, entry.getValue());
			}

			// --- write registered subclasses and their serializers, in registration order

			out.writeInt(registeredSubclassesToSerializerConfigSnapshots.size());
			for (Map.Entry<Class<?>, TypeSerializerConfigSnapshot> entry
					: registeredSubclassesToSerializerConfigSnapshots.entrySet()) {
				out.writeUTF(entry.getKey().getName());
				TypeSerializerUtil.writeSerializerConfigSnapshot(out, entry.getValue());
			}

			// --- write snapshot of non-registered subclass serializer cache

			out.writeInt(nonRegisteredSubclassesToSerializerConfigSnapshots.size());
			for (Map.Entry<Class<?>, TypeSerializerConfigSnapshot> entry
					: nonRegisteredSubclassesToSerializerConfigSnapshots.entrySet()) {
				out.writeUTF(entry.getKey().getName());
				TypeSerializerUtil.writeSerializerConfigSnapshot(out, entry.getValue());
			}
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			// --- read fields and their serializers, in order

			int numFields = in.readInt();
			this.fieldToSerializerConfigSnapshot = new LinkedHashMap<>(numFields);
			String fieldName;
			Field field;
			for (int i = 0; i < numFields; i++) {
				fieldName = in.readUTF();

				// search all superclasses for the field
				Class<?> clazz = getTypeClass();
				field = null;
				while (clazz != null) {
					try {
						field = clazz.getDeclaredField(fieldName);
						field.setAccessible(true);
						break;
					} catch (NoSuchFieldException e) {
						clazz = clazz.getSuperclass();
					}
				}

				if (field == null) {
					// the field no longer exists in the POJO
					throw new IOException("Can't find field " + fieldName + " in POJO class " + getTypeClass().getName());
				} else {
					fieldToSerializerConfigSnapshot.put(
							field,
							TypeSerializerUtil.readSerializerConfigSnapshot(in, getUserCodeClassLoader()));
				}
			}

			// --- read registered subclasses and their serializers, in registration order

			int numRegisteredSubclasses = in.readInt();
			this.registeredSubclassesToSerializerConfigSnapshots = new LinkedHashMap<>(numRegisteredSubclasses);
			String registeredSubclassname;
			Class<?> registeredSubclass;
			for (int i = 0; i < numRegisteredSubclasses; i++) {
				registeredSubclassname = in.readUTF();
				try {
					registeredSubclass = Class.forName(registeredSubclassname, true, getUserCodeClassLoader());
				} catch (ClassNotFoundException e) {
					throw new IOException("Cannot find requested class " + registeredSubclassname + " in classpath.", e);
				}

				this.registeredSubclassesToSerializerConfigSnapshots.put(
						registeredSubclass,
						TypeSerializerUtil.readSerializerConfigSnapshot(in, getUserCodeClassLoader()));
			}

			// --- read snapshot of non-registered subclass serializer cache

			int numCachedSubclassSerializers = in.readInt();
			this.nonRegisteredSubclassesToSerializerConfigSnapshots = new HashMap<>(numCachedSubclassSerializers);
			String cachedSubclassname;
			Class<?> cachedSubclass;
			for (int i = 0; i < numCachedSubclassSerializers; i++) {
				cachedSubclassname = in.readUTF();
				try {
					cachedSubclass = Class.forName(cachedSubclassname, true, getUserCodeClassLoader());
				} catch (ClassNotFoundException e) {
					throw new IOException("Cannot find requested class " + cachedSubclassname + " in classpath.", e);
				}

				this.nonRegisteredSubclassesToSerializerConfigSnapshots.put(
						cachedSubclass,
						TypeSerializerUtil.readSerializerConfigSnapshot(in, getUserCodeClassLoader()));
			}
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		public LinkedHashMap<Field, TypeSerializerConfigSnapshot> getFieldToSerializerConfigSnapshot() {
			return fieldToSerializerConfigSnapshot;
		}

		public LinkedHashMap<Class<?>, TypeSerializerConfigSnapshot> getRegisteredSubclassesToSerializerConfigSnapshots() {
			return registeredSubclassesToSerializerConfigSnapshots;
		}

		public HashMap<Class<?>, TypeSerializerConfigSnapshot> getNonRegisteredSubclassesToSerializerConfigSnapshots() {
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

	private void writeObject(ObjectOutputStream out)
		throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		out.writeInt(fields.length);
		for (Field field: fields) {
			FieldSerializer.serializeField(field, out);
		}
	}

	private void readObject(ObjectInputStream in)
		throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		int numFields = in.readInt();
		fields = new Field[numFields];
		for (int i = 0; i < numFields; i++) {
			fields[i] = FieldSerializer.deserializeField(in);
		}

		cl = Thread.currentThread().getContextClassLoader();
		subclassSerializerCache = new HashMap<Class<?>, TypeSerializer<?>>();
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
	private int findField(Field f) {
		int foundIndex = 0;
		for (Field field : fields) {
			if (f.equals(field)) {
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
	private static <T> PojoSerializerConfigSnapshot<T> buildConfigSnapshot(
			Class<T> pojoType,
			LinkedHashMap<Class<?>, Integer> registeredSubclassesToTags,
			TypeSerializer<?>[] registeredSubclassSerializers,
			Field[] fields,
			TypeSerializer<?>[] fieldSerializers,
			HashMap<Class<?>, TypeSerializer<?>> nonRegisteredSubclassSerializerCache) {

		final LinkedHashMap<Field, TypeSerializerConfigSnapshot> fieldToSerializerConfigSnapshots =
			new LinkedHashMap<>(fields.length);

		for (int i = 0; i < fields.length; i++) {
			fieldToSerializerConfigSnapshots.put(fields[i], fieldSerializers[i].snapshotConfiguration());
		}

		final LinkedHashMap<Class<?>, TypeSerializerConfigSnapshot> registeredSubclassesToSerializerConfigSnapshots =
				new LinkedHashMap<>(registeredSubclassesToTags.size());

		for (Map.Entry<Class<?>, Integer> entry : registeredSubclassesToTags.entrySet()) {
			registeredSubclassesToSerializerConfigSnapshots.put(
					entry.getKey(),
					registeredSubclassSerializers[entry.getValue()].snapshotConfiguration());
		}

		final HashMap<Class<?>, TypeSerializerConfigSnapshot> nonRegisteredSubclassesToSerializerConfigSnapshots =
				new LinkedHashMap<>(nonRegisteredSubclassSerializerCache.size());

		for (Map.Entry<Class<?>, TypeSerializer<?>> entry : nonRegisteredSubclassSerializerCache.entrySet()) {
			nonRegisteredSubclassesToSerializerConfigSnapshots.put(entry.getKey(), entry.getValue().snapshotConfiguration());
		}

		return new PojoSerializerConfigSnapshot<>(
				pojoType,
				fieldToSerializerConfigSnapshots,
				registeredSubclassesToSerializerConfigSnapshots,
				nonRegisteredSubclassesToSerializerConfigSnapshots);
	}

	// --------------------------------------------------------------------------------------------
	// Test utilities
	// --------------------------------------------------------------------------------------------

	@VisibleForTesting
	Field[] getFields() {
		return fields;
	}

	@VisibleForTesting
	TypeSerializer<?>[] getFieldSerializers() {
		return fieldSerializers;
	}

	@VisibleForTesting
	LinkedHashMap<Class<?>, Integer> getRegisteredClasses() {
		return registeredClasses;
	}

	@VisibleForTesting
	TypeSerializer<?>[] getRegisteredSerializers() {
		return registeredSerializers;
	}

	@VisibleForTesting
	HashMap<Class<?>, TypeSerializer<?>> getSubclassSerializerCache() {
		return subclassSerializerCache;
	}
}
