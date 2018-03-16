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
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.avro.utils.DataInputDecoder;
import org.apache.flink.formats.avro.utils.DataOutputEncoder;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import com.esotericsoftware.kryo.Kryo;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Old deprecated Avro serializer. It is retained for a smoother experience when
 * upgrading from an earlier Flink savepoint that stored this serializer.
 */
@Internal
@Deprecated
@SuppressWarnings({"unused", "deprecation"})
public final class AvroSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	private final Class<T> type;

	private final Class<? extends T> typeToInstantiate;

	/**
	 * Map of class tag (using classname as tag) to their Kryo registration.
	 *
	 * <p>This map serves as a preview of the final registration result of
	 * the Kryo instance, taking into account registration overwrites.
	 */
	private LinkedHashMap<String, KryoRegistration> kryoRegistrations;

	private transient ReflectDatumWriter<T> writer;
	private transient ReflectDatumReader<T> reader;

	private transient DataOutputEncoder encoder;
	private transient DataInputDecoder decoder;

	private transient Kryo kryo;

	private transient T deepCopyInstance;

	// --------------------------------------------------------------------------------------------

	public AvroSerializer(Class<T> type) {
		this(type, type);
	}

	public AvroSerializer(Class<T> type, Class<? extends T> typeToInstantiate) {
		this.type = checkNotNull(type);
		this.typeToInstantiate = checkNotNull(typeToInstantiate);

		InstantiationUtil.checkForInstantiation(typeToInstantiate);

		this.kryoRegistrations = buildKryoRegistrations(type);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public AvroSerializer<T> duplicate() {
		return new AvroSerializer<>(type, typeToInstantiate);
	}

	@Override
	public T createInstance() {
		return InstantiationUtil.instantiate(this.typeToInstantiate);
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
		checkAvroInitialized();
		this.encoder.setOut(target);
		this.writer.write(value, this.encoder);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		checkAvroInitialized();
		this.decoder.setIn(source);
		return this.reader.read(null, this.decoder);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		checkAvroInitialized();
		this.decoder.setIn(source);
		return this.reader.read(reuse, this.decoder);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		checkAvroInitialized();

		if (this.deepCopyInstance == null) {
			this.deepCopyInstance = InstantiationUtil.instantiate(type, Object.class);
		}

		this.decoder.setIn(source);
		this.encoder.setOut(target);

		T tmp = this.reader.read(this.deepCopyInstance, this.decoder);
		this.writer.write(tmp, this.encoder);
	}

	private void checkAvroInitialized() {
		if (this.reader == null) {
			this.reader = new ReflectDatumReader<>(type);
			this.writer = new ReflectDatumWriter<>(type);
			this.encoder = new DataOutputEncoder();
			this.decoder = new DataInputDecoder();
		}
	}

	private void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();

			Kryo.DefaultInstantiatorStrategy instantiatorStrategy = new Kryo.DefaultInstantiatorStrategy();
			instantiatorStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.setInstantiatorStrategy(instantiatorStrategy);

			kryo.setAsmEnabled(true);

			KryoUtils.applyRegistrations(kryo, kryoRegistrations.values());
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return 31 * this.type.hashCode() + this.typeToInstantiate.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AvroSerializer) {
			@SuppressWarnings("unchecked")
			AvroSerializer<T> avroSerializer = (AvroSerializer<T>) obj;

			return avroSerializer.canEqual(this) &&
					type == avroSerializer.type &&
					typeToInstantiate == avroSerializer.typeToInstantiate;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof AvroSerializer;
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public AvroSerializerConfigSnapshot<T> snapshotConfiguration() {
		return new AvroSerializerConfigSnapshot<>(type, typeToInstantiate, kryoRegistrations);
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof AvroSerializerConfigSnapshot) {
			final AvroSerializerConfigSnapshot<T> config = (AvroSerializerConfigSnapshot<T>) configSnapshot;

			if (type.equals(config.getTypeClass()) && typeToInstantiate.equals(config.getTypeToInstantiate())) {
				// resolve Kryo registrations; currently, since the Kryo registrations in Avro
				// are fixed, there shouldn't be a problem with the resolution here.

				LinkedHashMap<String, KryoRegistration> oldRegistrations = config.getKryoRegistrations();
				oldRegistrations.putAll(kryoRegistrations);

				for (Map.Entry<String, KryoRegistration> reconfiguredRegistrationEntry : kryoRegistrations.entrySet()) {
					if (reconfiguredRegistrationEntry.getValue().isDummy()) {
						return CompatibilityResult.requiresMigration();
					}
				}

				this.kryoRegistrations = oldRegistrations;
				return CompatibilityResult.compatible();
			}
		}

		// ends up here if the preceding serializer is not
		// the ValueSerializer, or serialized data type has changed
		return CompatibilityResult.requiresMigration();
	}

	/**
	 * Config snapshot for this serializer.
	 */
	public static class AvroSerializerConfigSnapshot<T> extends KryoRegistrationSerializerConfigSnapshot<T> {

		private static final int VERSION = 1;

		private Class<? extends T> typeToInstantiate;

		public AvroSerializerConfigSnapshot() {}

		public AvroSerializerConfigSnapshot(
				Class<T> baseType,
				Class<? extends T> typeToInstantiate,
				LinkedHashMap<String, KryoRegistration> kryoRegistrations) {

			super(baseType, kryoRegistrations);
			this.typeToInstantiate = Preconditions.checkNotNull(typeToInstantiate);
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			out.writeUTF(typeToInstantiate.getName());
		}

		@SuppressWarnings("unchecked")
		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			String classname = in.readUTF();
			try {
				typeToInstantiate = (Class<? extends T>) Class.forName(classname, true, getUserCodeClassLoader());
			} catch (ClassNotFoundException e) {
				throw new IOException("Cannot find requested class " + classname + " in classpath.", e);
			}
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		public Class<? extends T> getTypeToInstantiate() {
			return typeToInstantiate;
		}
	}

	// --------------------------------------------------------------------------------------------

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		// kryoRegistrations may be null if this Avro serializer is deserialized from an old version
		if (kryoRegistrations == null) {
			this.kryoRegistrations = buildKryoRegistrations(type);
		}
	}

	private static <T> LinkedHashMap<String, KryoRegistration> buildKryoRegistrations(Class<T> serializedDataType) {
		final LinkedHashMap<String, KryoRegistration> registrations = new LinkedHashMap<>();

		// register Avro types.
		registrations.put(
				GenericData.Array.class.getName(),
				new KryoRegistration(
						GenericData.Array.class,
						new ExecutionConfig.SerializableSerializer<>(new Serializers.SpecificInstanceCollectionSerializerForArrayList())));
		registrations.put(Utf8.class.getName(), new KryoRegistration(Utf8.class));
		registrations.put(GenericData.EnumSymbol.class.getName(), new KryoRegistration(GenericData.EnumSymbol.class));
		registrations.put(GenericData.Fixed.class.getName(), new KryoRegistration(GenericData.Fixed.class));
		registrations.put(GenericData.StringType.class.getName(), new KryoRegistration(GenericData.StringType.class));

		// register the serialized data type
		registrations.put(serializedDataType.getName(), new KryoRegistration(serializedDataType));

		return registrations;
	}
}
