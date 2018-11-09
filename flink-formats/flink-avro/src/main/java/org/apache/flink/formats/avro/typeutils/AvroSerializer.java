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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.avro.utils.DataInputDecoder;
import org.apache.flink.formats.avro.utils.DataOutputEncoder;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer that serializes types via Avro.
 *
 * <p>The serializer supports:
 * <ul>
 * <li>efficient specific record serialization for types generated via Avro</li>
 * <li>serialization via reflection (ReflectDatumReader / -Writer)</li>
 * <li>serialization of generic records via GenericDatumReader / -Writer</li>
 * </ul>
 * The serializer instantiates them depending on the class of the type it should serialize.
 *
 * <p><b>Important:</b> This serializer is NOT THREAD SAFE, because it reuses the data encoders
 * and decoders which have buffers that would be shared between the threads if used concurrently
 *
 * @param <T> The type to be serialized.
 */
public class AvroSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 2L;

	/** Logger instance.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(AvroSerializer.class);

	/** Flag whether to check for concurrent thread access.
	 * Because this flag is static final, a value of 'false' allows the JIT compiler to eliminate
	 * the guarded code sections.
	 */
	private static final boolean CONCURRENT_ACCESS_CHECK =
		LOG.isDebugEnabled() || AvroSerializerDebugInitHelper.setToDebug;

	// -------- configuration fields, serializable -----------

	/** The class of the type that is serialized by this serializer.
	 */
	private final Class<T> type;
	private final SerializableAvroSchema schema;
	private final SerializableAvroSchema previousSchema;

	// -------- runtime fields, non-serializable, lazily initialized -----------

	private transient GenericData avroData;
	private transient DatumWriter<T> writer;
	private transient DataOutputEncoder encoder;
	private transient DataInputDecoder decoder;
	private transient DatumReader<T> reader;
	private transient Schema runtimeSchema;

	/** The serializer configuration snapshot, cached for efficiency.
	 */
	private transient TypeSerializerSnapshot<T> configSnapshot;

	/** The currently accessing thread, set and checked on debug level only.
	 */
	private transient volatile Thread currentThread;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new AvroSerializer for the type indicated by the given class.
	 * This constructor is intended to be used with {@link SpecificRecord} or reflection serializer.
	 * For serializing {@link GenericData.Record} use {@link AvroSerializer#AvroSerializer(Class, Schema)}
	 */
	public AvroSerializer(Class<T> type) {
		this(checkNotNull(type), new SerializableAvroSchema(), new SerializableAvroSchema());
		checkArgument(!isGenericRecord(type),
			"For GenericData.Record use constructor with explicit schema.");
	}

	/**
	 * Creates a new AvroSerializer for the type indicated by the given class.
	 * This constructor is expected to be used only with {@link GenericData.Record}.
	 * For {@link SpecificRecord} or reflection serializer use
	 * {@link AvroSerializer#AvroSerializer(Class)}
	 */
	public AvroSerializer(Class<T> type, Schema schema) {
		this(checkNotNull(type), new SerializableAvroSchema(checkNotNull(schema)), new SerializableAvroSchema());
		checkArgument(isGenericRecord(type),
			"For classes other than GenericData.Record use constructor without explicit schema.");
	}

	/**
	 * Creates a new AvroSerializer for the type indicated by the given class.
	 */
	@Internal
	AvroSerializer(Class<T> type, @Nullable SerializableAvroSchema newSchema, @Nullable SerializableAvroSchema previousSchema) {
		this.type = checkNotNull(type);
		this.schema = newSchema;
		this.previousSchema = previousSchema;
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
			return avroData.deepCopy(runtimeSchema, from);
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
	public TypeSerializerSnapshot<T> snapshotConfiguration() {
		if (configSnapshot == null) {
			checkAvroInitialized();
			configSnapshot = new AvroSerializerSnapshot<>(runtimeSchema, type);
		}
		return configSnapshot;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	static boolean isGenericRecord(Class<?> type) {
		return !SpecificRecord.class.isAssignableFrom(type) &&
			GenericRecord.class.isAssignableFrom(type);
	}

	@Override
	public TypeSerializer<T> duplicate() {
		checkAvroInitialized();
		return new AvroSerializer<>(type, new SerializableAvroSchema(runtimeSchema), previousSchema);
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
		AvroFactory<T> factory = AvroFactory.create(type, schema.getAvroSchema(), previousSchema.getAvroSchema());
		this.runtimeSchema = factory.getSchema();
		this.writer = factory.getWriter();
		this.reader = factory.getReader();
		this.encoder = factory.getEncoder();
		this.decoder = factory.getDecoder();
		this.avroData = factory.getAvroData();
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

	Schema getAvroSchema() {
		checkAvroInitialized();
		return runtimeSchema;
	}

	// ------------------------------------------------------------------------
	//  Serializer Snapshots
	// ------------------------------------------------------------------------

	/**
	 * A config snapshot for the Avro Serializer that stores the Avro Schema to check compatibility.
	 * This class is now deprecated and only kept for backward comparability.
	 */
	@Deprecated
	public static final class AvroSchemaSerializerConfigSnapshot<T> extends AvroSerializerSnapshot<T> {

		public AvroSchemaSerializerConfigSnapshot() {
		}

	}
}
