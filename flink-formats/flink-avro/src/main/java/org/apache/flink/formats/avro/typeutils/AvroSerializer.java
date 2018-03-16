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

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistrationSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.avro.utils.DataInputDecoder;
import org.apache.flink.formats.avro.utils.DataOutputEncoder;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer that serializes types via Avro.
 *
 * <p>The serializer supports both efficient specific record serialization for
 * types generated via Avro, as well as serialization via reflection
 * (ReflectDatumReader / -Writer). The serializer instantiates them depending on
 * the class of the type it should serialize.
 *
 * <p><b>Important:</b> This serializer is NOT THREAD SAFE, because it reuses the data encoders
 * and decoders which have buffers that would be shared between the threads if used concurrently
 *
 * @param <T> The type to be serialized.
 */
public class AvroSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	/** Logger instance. */
	private static final Logger LOG = LoggerFactory.getLogger(AvroSerializer.class);

	/** Flag whether to check for concurrent thread access.
	 * Because this flag is static final, a value of 'false' allows the JIT compiler to eliminate
	 * the guarded code sections. */
	private static final boolean CONCURRENT_ACCESS_CHECK =
			LOG.isDebugEnabled() || AvroSerializerDebugInitHelper.setToDebug;

	// -------- configuration fields, serializable -----------

	/** The class of the type that is serialized by this serializer. */
	private final Class<T> type;

	// -------- runtime fields, non-serializable, lazily initialized -----------

	private transient SpecificDatumWriter<T> writer;
	private transient SpecificDatumReader<T> reader;

	private transient DataOutputEncoder encoder;
	private transient DataInputDecoder decoder;

	private transient SpecificData avroData;

	private transient Schema schema;

	/** The serializer configuration snapshot, cached for efficiency. */
	private transient AvroSchemaSerializerConfigSnapshot configSnapshot;

	/** The currently accessing thread, set and checked on debug level only. */
	private transient volatile Thread currentThread;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new AvroSerializer for the type indicated by the given class.
	 */
	public AvroSerializer(Class<T> type) {
		this.type = checkNotNull(type);
	}

	/**
	 * @deprecated Use {@link AvroSerializer#AvroSerializer(Class)} instead.
	 */
	@Deprecated
	@SuppressWarnings("unused")
	public AvroSerializer(Class<T> type, Class<? extends T> typeToInstantiate) {
		this(type);
	}

	// ------------------------------------------------------------------------

	public Class<T> getType() {
		return type;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		return -1;
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	@Override
	public T createInstance() {
		return InstantiationUtil.instantiate(type);
	}

	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		if (CONCURRENT_ACCESS_CHECK) {
			enterExclusiveThread();
		}

		try {
			checkAvroInitialized();
			this.encoder.setOut(target);
			this.writer.write(value, this.encoder);
		}
		finally {
			if (CONCURRENT_ACCESS_CHECK) {
				exitExclusiveThread();
			}
		}
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		if (CONCURRENT_ACCESS_CHECK) {
			enterExclusiveThread();
		}

		try {
			checkAvroInitialized();
			this.decoder.setIn(source);
			return this.reader.read(null, this.decoder);
		}
		finally {
			if (CONCURRENT_ACCESS_CHECK) {
				exitExclusiveThread();
			}
		}
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		if (CONCURRENT_ACCESS_CHECK) {
			enterExclusiveThread();
		}

		try {
			checkAvroInitialized();
			this.decoder.setIn(source);
			return this.reader.read(reuse, this.decoder);
		}
		finally {
			if (CONCURRENT_ACCESS_CHECK) {
				exitExclusiveThread();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Copying
	// ------------------------------------------------------------------------

	@Override
	public T copy(T from) {
		if (CONCURRENT_ACCESS_CHECK) {
			enterExclusiveThread();
		}

		try {
			checkAvroInitialized();
			return avroData.deepCopy(schema, from);
		}
		finally {
			if (CONCURRENT_ACCESS_CHECK) {
				exitExclusiveThread();
			}
		}
	}

	@Override
	public T copy(T from, T reuse) {
		return copy(from);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// we do not have concurrency checks here, because serialize() and
		// deserialize() do the checks and the current concurrency check mechanism
		// does provide additional safety in cases of re-entrant calls
		serialize(deserialize(source), target);
	}

	// ------------------------------------------------------------------------
	//  Compatibility and Upgrades
	// ------------------------------------------------------------------------

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		if (configSnapshot == null) {
			checkAvroInitialized();
			configSnapshot = new AvroSchemaSerializerConfigSnapshot(schema.toString(false));
		}
		return configSnapshot;
	}

	@Override
	@SuppressWarnings("deprecation")
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof AvroSchemaSerializerConfigSnapshot) {
			// proper schema snapshot, can do the sophisticated schema-based compatibility check
			final String schemaString = ((AvroSchemaSerializerConfigSnapshot) configSnapshot).getSchemaString();
			final Schema lastSchema = new Schema.Parser().parse(schemaString);

			checkAvroInitialized();
			final SchemaPairCompatibility compatibility =
					SchemaCompatibility.checkReaderWriterCompatibility(schema, lastSchema);

			return compatibility.getType() == SchemaCompatibilityType.COMPATIBLE ?
					CompatibilityResult.compatible() : CompatibilityResult.requiresMigration();
		}
		else if (configSnapshot instanceof AvroSerializerConfigSnapshot) {
			// old snapshot case, just compare the type
			// we don't need to restore any Kryo stuff, since Kryo was never used for persistence,
			// only for object-to-object copies.
			final AvroSerializerConfigSnapshot old = (AvroSerializerConfigSnapshot) configSnapshot;
			return type.equals(old.getTypeClass()) ?
					CompatibilityResult.compatible() : CompatibilityResult.requiresMigration();
		}
		else {
			return CompatibilityResult.requiresMigration();
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public TypeSerializer<T> duplicate() {
		return new AvroSerializer<>(type);
	}

	@Override
	public int hashCode() {
		return 42 + type.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj != null && obj.getClass() == AvroSerializer.class) {
			final AvroSerializer that = (AvroSerializer) obj;
			return this.type == that.type;
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
		return getClass().getName() + " (" + getType().getName() + ')';
	}

	// ------------------------------------------------------------------------
	//  Initialization
	// ------------------------------------------------------------------------

	private void checkAvroInitialized() {
		if (writer == null) {
			initializeAvro();
		}
	}

	private void initializeAvro() {
		final ClassLoader cl = Thread.currentThread().getContextClassLoader();

		if (SpecificRecord.class.isAssignableFrom(type)) {
			this.avroData = new SpecificData(cl);
			this.schema = this.avroData.getSchema(type);
			this.reader = new SpecificDatumReader<>(schema, schema, avroData);
			this.writer = new SpecificDatumWriter<>(schema, avroData);
		}
		else {
			final ReflectData reflectData = new ReflectData(cl);
			this.avroData = reflectData;
			this.schema = this.avroData.getSchema(type);
			this.reader = new ReflectDatumReader<>(schema, schema, reflectData);
			this.writer = new ReflectDatumWriter<>(schema, reflectData);
		}

		this.encoder = new DataOutputEncoder();
		this.decoder = new DataInputDecoder();
	}

	// --------------------------------------------------------------------------------------------
	//  Concurrency checks
	// --------------------------------------------------------------------------------------------

	private void enterExclusiveThread() {
		// we use simple get, check, set here, rather than CAS
		// we don't need lock-style correctness, this is only a sanity-check and we thus
		// favor speed at the cost of some false negatives in this check
		Thread previous = currentThread;
		Thread thisThread = Thread.currentThread();

		if (previous == null) {
			currentThread = thisThread;
		}
		else if (previous != thisThread) {
			throw new IllegalStateException(
					"Concurrent access to KryoSerializer. Thread 1: " + thisThread.getName() +
							" , Thread 2: " + previous.getName());
		}
	}

	private void exitExclusiveThread() {
		currentThread = null;
	}

	// ------------------------------------------------------------------------
	//  Serializer Snapshots
	// ------------------------------------------------------------------------

	/**
	 * A config snapshot for the Avro Serializer that stores the Avro Schema to check compatibility.
	 */
	public static final class AvroSchemaSerializerConfigSnapshot extends TypeSerializerConfigSnapshot {

		private String schemaString;

		/**
		 * Default constructor for instantiation via reflection.
		 */
		@SuppressWarnings("unused")
		public AvroSchemaSerializerConfigSnapshot() {}

		public AvroSchemaSerializerConfigSnapshot(String schemaString) {
			this.schemaString = checkNotNull(schemaString);
		}

		public String getSchemaString() {
			return schemaString;
		}

		// --- Serialization ---

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);
			this.schemaString = in.readUTF();
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);
			out.writeUTF(schemaString);
		}

		// --- Version ---

		@Override
		public int getVersion() {
			return 1;
		}

		// --- Utils ---

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			else if (obj != null && obj.getClass() == AvroSchemaSerializerConfigSnapshot.class) {
				final AvroSchemaSerializerConfigSnapshot that = (AvroSchemaSerializerConfigSnapshot) obj;
				return this.schemaString.equals(that.schemaString);
			}
			else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return 11 + schemaString.hashCode();
		}

		@Override
		public String toString() {
			return getClass().getName() + " (" + schemaString + ')';
		}
	}

	/**
	 * The outdated config snapshot, retained for backwards compatibility.
	 *
	 * @deprecated The {@link AvroSchemaSerializerConfigSnapshot} should be used instead.
	 */
	@Deprecated
	public static class AvroSerializerConfigSnapshot<T> extends KryoRegistrationSerializerConfigSnapshot<T> {

		private static final int VERSION = 1;

		private Class<? extends T> typeToInstantiate;

		public AvroSerializerConfigSnapshot() {}

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
}
