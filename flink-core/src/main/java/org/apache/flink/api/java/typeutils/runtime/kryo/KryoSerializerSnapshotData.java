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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig.SerializableSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import com.esotericsoftware.kryo.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InvalidClassException;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.api.java.typeutils.runtime.kryo.OptionalMap.optionalMapOf;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

final class KryoSerializerSnapshotData<T> {

	private static final Logger LOG = LoggerFactory.getLogger(KryoSerializerSnapshotData.class);

	// --------------------------------------------------------------------------------------------
	// Factories
	// --------------------------------------------------------------------------------------------

	static <T> KryoSerializerSnapshotData<T> createFrom(
		Class<T> typeClass,
		LinkedHashMap<Class<?>, SerializableSerializer<?>> defaultKryoSerializers,
		LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses,
		LinkedHashMap<String, KryoRegistration> kryoRegistrations) {

		return new KryoSerializerSnapshotData<>(
			typeClass,
			optionalMapOf(defaultKryoSerializers, Class::getName),
			optionalMapOf(defaultKryoSerializerClasses, Class::getName),
			optionalMapOf(kryoRegistrations, Function.identity()));
	}

	static <T> KryoSerializerSnapshotData<T> createFrom(DataInputView in, ClassLoader cl) throws IOException {
		Class<T> typeClass = readTypeClass(in, cl);
		OptionalMap<String, KryoRegistration> kryoRegistrations = readKryoRegistrations(in, cl);
		OptionalMap<Class<?>, SerializableSerializer<?>> defaultSerializer = readDefaultKryoSerializers(in, cl);
		OptionalMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClasses =
			readDefaultKryoSerializerClasses(in, cl);

		return new KryoSerializerSnapshotData<>(
			typeClass,
			defaultSerializer,
			defaultSerializerClasses,
			kryoRegistrations);
	}

	// --------------------------------------------------------------------------------------------
	// Fields
	// --------------------------------------------------------------------------------------------

	private final Class<T> typeClass;
	private final OptionalMap<Class<?>, SerializableSerializer<?>> defaultKryoSerializers;
	private final OptionalMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses;
	private final OptionalMap<String, KryoRegistration> kryoRegistrations;

	private KryoSerializerSnapshotData(Class<T> typeClass,
									   OptionalMap<Class<?>, SerializableSerializer<?>> defaultKryoSerializers,
									   OptionalMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses,
									   OptionalMap<String, KryoRegistration> kryoRegistrations) {

		this.typeClass = typeClass;
		this.defaultKryoSerializers = defaultKryoSerializers;
		this.defaultKryoSerializerClasses = defaultKryoSerializerClasses;
		this.kryoRegistrations = kryoRegistrations;
	}

	// --------------------------------------------------------------------------------------------
	// Getters
	// --------------------------------------------------------------------------------------------

	Class<T> getTypeClass() {
		return typeClass;
	}

	OptionalMap<Class<?>, SerializableSerializer<?>> getDefaultKryoSerializers() {
		return defaultKryoSerializers;
	}

	OptionalMap<Class<?>, Class<? extends Serializer<?>>> getDefaultKryoSerializerClasses() {
		return defaultKryoSerializerClasses;
	}

	OptionalMap<String, KryoRegistration> getKryoRegistrations() {
		return kryoRegistrations;
	}

	// --------------------------------------------------------------------------------------------
	// Write
	// --------------------------------------------------------------------------------------------

	void writeSnapshotData(DataOutputView out) throws IOException {
		writeTypeClass(out);
		writeKryoRegistrations(out, kryoRegistrations);
		writeDefaultKryoSerializers(out, defaultKryoSerializers);
		writeDefaultKryoSerializerClasses(out, defaultKryoSerializerClasses);
	}

	private void writeTypeClass(DataOutputView out) throws IOException {
		out.writeUTF(typeClass.getName());
	}

	private static void writeKryoRegistrations(
		DataOutputView out,
		OptionalMap<String, KryoRegistration> kryoRegistrations) throws IOException {

		out.writeInt(kryoRegistrations.size());
		for (Entry<String, KryoRegistration> entry : kryoRegistrations.unwrapOptionals().entrySet()) {
			out.writeUTF(entry.getKey());
			new KryoRegistrationSerializationProxy<>(entry.getValue()).write(out);
		}
	}

	private void writeDefaultKryoSerializers(
		DataOutputView out,
		OptionalMap<Class<?>,
			SerializableSerializer<?>> defaultKryoSerializers) throws IOException {

		out.writeInt(defaultKryoSerializers.size());
		for (Entry<Class<?>, SerializableSerializer<?>> entry : defaultKryoSerializers.unwrapOptionals().entrySet()) {
			Class<?> javaClass = entry.getKey();
			SerializableSerializer<?> serializerInstance = entry.getValue();

			out.writeUTF(javaClass.getName());
			InstantiationUtil.serializeObject(new DataOutputViewStream(out), serializerInstance);
		}
	}

	@SuppressWarnings("unchecked")
	private static void writeDefaultKryoSerializerClasses(
		DataOutputView out,
		OptionalMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses)
		throws IOException {

		out.writeInt(defaultKryoSerializerClasses.size());

		for (Entry<Class<?>, Class<? extends Serializer<?>>> entry : defaultKryoSerializerClasses.unwrapOptionals().entrySet()) {
			Class<?> javaClass = entry.getKey();
			Class<? extends Serializer<?>> serializerClass = entry.getValue();
			out.writeUTF(javaClass.getName());
			out.writeUTF(serializerClass.getName());
		}
	}

	// --------------------------------------------------------------------------------------------
	// Read
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static <T> Class<T> readTypeClass(DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		String genericTypeClassname = in.readUTF();
		try {
			return (Class<T>) Class.forName(genericTypeClassname, true, userCodeClassLoader);
		}
		catch (ClassNotFoundException e) {
			throw new IOException("Could not find the requested class " + genericTypeClassname + " in classpath.", e);
		}
	}

	private static OptionalMap<String, KryoRegistration> readKryoRegistrations(
		DataInputView in,
		ClassLoader userCodeClassLoader) throws IOException {

		KryoRegistrationSerializationProxy<?> proxy = new KryoRegistrationSerializationProxy<>(userCodeClassLoader);

		OptionalMap<String, KryoRegistration> registrations = new OptionalMap<>();
		final int size = in.readInt();
		for (int i = 0; i < size; i++) {
			final String name = in.readUTF();
			proxy.read(in);
			registrations.put(name, name, proxy.kryoRegistration.orElse(null));
		}

		return registrations;
	}

	private static OptionalMap<Class<?>, SerializableSerializer<?>> readDefaultKryoSerializers(DataInputView in, ClassLoader cl) throws IOException {
		OptionalMap<Class<?>, SerializableSerializer<?>> kryoSerializers = new OptionalMap<>();
		final int size = in.readInt();
		for (int i = 0; i < size; i++) {
			final String className = in.readUTF();
			Class<?> javaClass = null;
			try {
				javaClass = Class.forName(className, false, cl);
			}
			catch (ClassNotFoundException e) {
				LOG.warn("Cannot find registered class " + className + " for Kryo serialization in classpath.", e);
			}
			SerializableSerializer<?> kryoSerializer = null;
			try {
				kryoSerializer = InstantiationUtil.deserializeObject(new DataInputViewStream(in), cl);
			}
			catch (Throwable e) {
				LOG.warn("Cannot deserialize a previously serialized kryo serializer for the type " + className, e);
			}
			kryoSerializers.put(className, javaClass, kryoSerializer);
		}
		return kryoSerializers;
	}

	@SuppressWarnings("unchecked")
	private static OptionalMap<Class<?>, Class<? extends Serializer<?>>> readDefaultKryoSerializerClasses(
		DataInputView in,
		ClassLoader cl) throws IOException {

		OptionalMap<Class<?>, Class<? extends Serializer<?>>> kryoSerializerClasses = new OptionalMap<>();
		final int size = in.readInt();
		for (int i = 0; i < size; i++) {
			final String className = in.readUTF();
			Class<?> typeClass = null;
			try {
				typeClass = Class.forName(className, false, cl);
			}
			catch (ClassNotFoundException e) {
				LOG.warn("Cannot find registered class " + className + " for Kryo serialization in classpath.", e);
			}
			final String kryoSerializerClassName = in.readUTF();
			Class<? extends Serializer<?>> kryoSerializerClass = null;
			try {
				kryoSerializerClass = (Class<? extends Serializer<?>>) Class.forName(kryoSerializerClassName, false, cl);
			}
			catch (Throwable e) {
				LOG.warn("Cannot find registered class " + className + " for Kryo serialization in classpath.", e);
			}
			kryoSerializerClasses.put(className, typeClass, kryoSerializerClass);
		}
		return kryoSerializerClasses;
	}

	// --------------------------------------------------------------------------------------------
	// Helper Classes
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	private static final class KryoRegistrationSerializationProxy<RC> {

		private ClassLoader userCodeClassLoader;

		private Optional<KryoRegistration> kryoRegistration = Optional.empty();

		private KryoRegistrationSerializationProxy(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		}

		private KryoRegistrationSerializationProxy(KryoRegistration kryoRegistration) {
			this.kryoRegistration = Optional.of(kryoRegistration);
		}

		public void write(DataOutputView out) throws IOException {
			checkState(kryoRegistration.isPresent());
			KryoRegistration kryoRegistration = this.kryoRegistration.get();
			out.writeUTF(kryoRegistration.getRegisteredClass().getName());

			final KryoRegistration.SerializerDefinitionType serializerDefinitionType =
				kryoRegistration.getSerializerDefinitionType();

			out.writeInt(serializerDefinitionType.ordinal());
			switch (serializerDefinitionType) {
				case UNSPECIFIED: {
					// nothing else to write
					break;
				}
				case CLASS: {
					Class<? extends Serializer<?>> serializerClass = kryoRegistration.getSerializerClass();
					assert serializerClass != null;
					out.writeUTF(serializerClass.getName());
					break;
				}
				case INSTANCE: {
					try (final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out)) {
						InstantiationUtil.serializeObject(outViewWrapper, kryoRegistration.getSerializableSerializerInstance());
					}
					break;
				}
				default: {
					throw new IllegalStateException(
						"Unrecognized Kryo registration serializer definition type: " + serializerDefinitionType);
				}
			}
		}

		@SuppressWarnings("unchecked")
		public void read(DataInputView in) throws IOException {
			this.kryoRegistration = Optional.empty();

			String registeredClassname = in.readUTF();
			Class<RC> registeredClass;
			try {
				registeredClass = (Class<RC>) Class.forName(registeredClassname, true, userCodeClassLoader);
			}
			catch (ClassNotFoundException e) {
				LOG.warn("Cannot find registered class " + registeredClassname + " for Kryo serialization in classpath;" +
					" using a dummy class as a placeholder.", e);
				return;
			}

			final KryoRegistration.SerializerDefinitionType serializerDefinitionType =
				KryoRegistration.SerializerDefinitionType.values()[in.readInt()];

			switch (serializerDefinitionType) {
				case UNSPECIFIED: {
					kryoRegistration = Optional.of(new KryoRegistration(registeredClass));
					break;
				}
				case CLASS: {
					String serializerClassname = in.readUTF();
					Class serializerClass;
					try {
						serializerClass = Class.forName(serializerClassname, true, userCodeClassLoader);
						this.kryoRegistration = Optional.of(new KryoRegistration(registeredClass, serializerClass));
					}
					catch (ClassNotFoundException e) {
						LOG.warn("Cannot find registered Kryo serializer class for class " + registeredClassname +
							" in classpath; using a dummy Kryo serializer that should be replaced as soon as" +
							" a new Kryo serializer for the class is present", e);
					}
					break;
				}
				case INSTANCE: {
					ExecutionConfig.SerializableSerializer<? extends Serializer<RC>> serializerInstance;

					try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
						serializerInstance = InstantiationUtil.deserializeObject(inViewWrapper, userCodeClassLoader);
						kryoRegistration = Optional.of(new KryoRegistration(registeredClass, serializerInstance));
					}
					catch (ClassNotFoundException e) {
						LOG.warn("Cannot find registered Kryo serializer class for class " + registeredClassname +
							" in classpath; using a dummy Kryo serializer that should be replaced as soon as" +
							" a new Kryo serializer for the class is present", e);
					}
					catch (InvalidClassException e) {
						LOG.warn("The registered Kryo serializer class for class " + registeredClassname +
							" has changed and is no longer valid; using a dummy Kryo serializer that should be replaced" +
							" as soon as a new Kryo serializer for the class is present.", e);

					}
					break;
				}
				default: {
					throw new IllegalStateException(
						"Unrecognized Kryo registration serializer definition type: " + serializerDefinitionType);
				}
			}
		}
	}
}
