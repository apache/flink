package org.apache.flink.streaming.connectors.fs;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;

/**
* Implementation of AvroKeyValue writer that can be used in Sink.
* Each entry would be wrapped in GenericRecord with key/value fields(same as in m/r lib)
<pre>
Usage:
{@code
		BucketingSink<Tuple2<Long, Long>> sink = new BucketingSink<Tuple2<Long, Long>>("/tmp/path");
		sink.setBucketer(new DateTimeBucketer<Tuple2<Long, Long>>("yyyy-MM-dd/HH/mm/"));
		sink.setPendingSuffix(".avro");
		Map<String, String> properties = new HashMap<>();
		Schema longSchema = Schema.create(Type.LONG);
		String keySchema = longSchema.toString();
		String valueSchema = longSchema.toString();
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_KEY_SCHEMA, keySchema);
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_VALUE_SCHEMA, valueSchema);
		properties.put(AvroKeyValueSinkWriter.CONF_COMPRESS, Boolean.toString(true));
		properties.put(AvroKeyValueSinkWriter.CONF_COMPRESS_CODEC, DataFileConstants.SNAPPY_CODEC);

		sink.setWriter(new AvroKeyValueSinkWriter<Long, Long>(properties));
		sink.setBatchSize(1024 * 1024 * 64); // this is 64 MB,
}
</pre>
*/
@Deprecated
public class AvroKeyValueSinkWriter<K, V> extends StreamWriterBase<Tuple2<K, V>> implements Writer<Tuple2<K, V>>, InputTypeConfigurable {
	private static final long serialVersionUID = 1L;
	public static final String CONF_OUTPUT_KEY_SCHEMA = "avro.schema.output.key";
	public static final String CONF_OUTPUT_VALUE_SCHEMA = "avro.schema.output.value";
	public static final String CONF_COMPRESS = FileOutputFormat.COMPRESS;
	public static final String CONF_COMPRESS_CODEC = FileOutputFormat.COMPRESS_CODEC;
	public static final String CONF_DEFLATE_LEVEL = "avro.deflate.level";
	public static final String CONF_XZ_LEVEL = "avro.xz.level";

	private transient AvroKeyValueWriter<K, V> keyValueWriter;

	private final Map<String, String> properties;

	/**
	 * C'tor for the writer.
	 *
	 * <p>You can provide different properties that will be used to configure avro key-value writer as simple properties map(see example above)
	 * @param properties
	 */
	@SuppressWarnings("deprecation")
	public AvroKeyValueSinkWriter(Map<String, String> properties) {
		this.properties = properties;
		validateProperties();
	}

	protected AvroKeyValueSinkWriter(AvroKeyValueSinkWriter<K, V> other) {
		super(other);
		this.properties = other.properties;
		validateProperties();
	}

	private void validateProperties() {
		String keySchemaString = properties.get(CONF_OUTPUT_KEY_SCHEMA);
		if (keySchemaString == null) {
			throw new IllegalStateException("No key schema provided, set '" + CONF_OUTPUT_KEY_SCHEMA + "' property");
		}
		Schema.parse(keySchemaString); //verifying that schema valid

		String valueSchemaString = properties.get(CONF_OUTPUT_VALUE_SCHEMA);
		if (valueSchemaString == null) {
			throw new IllegalStateException("No value schema provided, set '" + CONF_OUTPUT_VALUE_SCHEMA + "' property");
		}
		Schema.parse(valueSchemaString); //verifying that schema valid
	}

	private boolean getBoolean(Map<String, String> conf, String key, boolean def) {
		String value = conf.get(key);
		if (value == null) {
			return def;
		}
		return Boolean.parseBoolean(value);
	}

	private int getInt(Map<String, String> conf, String key, int def) {
		String value = conf.get(key);
		if (value == null) {
			return def;
		}
		return Integer.parseInt(value);
	}

	//this derived from AvroOutputFormatBase.getCompressionCodec(..)
	private CodecFactory getCompressionCodec(Map<String, String> conf) {
		if (getBoolean(conf, CONF_COMPRESS, false)) {
			int deflateLevel = getInt(conf, CONF_DEFLATE_LEVEL, CodecFactory.DEFAULT_DEFLATE_LEVEL);
			int xzLevel = getInt(conf, CONF_XZ_LEVEL, CodecFactory.DEFAULT_XZ_LEVEL);

			String outputCodec = conf.get(CONF_COMPRESS_CODEC);

			if (DataFileConstants.DEFLATE_CODEC.equals(outputCodec)) {
				return CodecFactory.deflateCodec(deflateLevel);
			} else if (DataFileConstants.XZ_CODEC.equals(outputCodec)) {
				return CodecFactory.xzCodec(xzLevel);
			} else {
				return CodecFactory.fromString(outputCodec);
			}
		}
		return CodecFactory.nullCodec();
	}

	@Override
	@SuppressWarnings("deprecation")
	public void open(FileSystem fs, Path path) throws IOException {
		super.open(fs, path);

		try {
			CodecFactory compressionCodec = getCompressionCodec(properties);
			Schema keySchema = Schema.parse(properties.get(CONF_OUTPUT_KEY_SCHEMA));
			Schema valueSchema = Schema.parse(properties.get(CONF_OUTPUT_VALUE_SCHEMA));
			keyValueWriter = new AvroKeyValueWriter<K, V>(
				keySchema,
				valueSchema,
				compressionCodec,
				getStream());
		} finally {
			if (keyValueWriter == null) {
				close();
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (keyValueWriter != null) {
			keyValueWriter.close();
		} else {
			// need to make sure we close this if we never created the Key/Value Writer.
			super.close();
		}
	}

	@Override
	public long flush() throws IOException {
		if (keyValueWriter != null) {
			keyValueWriter.sync();
		}
		return super.flush();
	}

	@Override
	public void write(Tuple2<K, V> element) throws IOException {
		getStream(); // Throws if the stream is not open
		keyValueWriter.write(element.f0, element.f1);
	}

	@Override
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (!type.isTupleType()) {
			throw new IllegalArgumentException("Input TypeInformation is not a tuple type.");
		}

		TupleTypeInfoBase<?> tupleType = (TupleTypeInfoBase<?>) type;

		if (tupleType.getArity() != 2) {
			throw new IllegalArgumentException("Input TypeInformation must be a Tuple2 type.");
		}
	}

	@Override
	public AvroKeyValueSinkWriter<K, V> duplicate() {
		return new AvroKeyValueSinkWriter<>(this);
	}

	// taken from m/r avro lib to remove dependency on it
	private static final class AvroKeyValueWriter<K, V> {
		/** A writer for the Avro container file. */
		private final DataFileWriter<GenericRecord> mAvroFileWriter;

		/**
		 * The writer schema for the generic record entries of the Avro
		 * container file.
		 */
		private final Schema mKeyValuePairSchema;

		/**
		 * A reusable Avro generic record for writing key/value pairs to the
		 * file.
		 */
		private final AvroKeyValue<Object, Object> mOutputRecord;

		AvroKeyValueWriter(Schema keySchema, Schema valueSchema,
				CodecFactory compressionCodec, OutputStream outputStream,
				int syncInterval) throws IOException {
			// Create the generic record schema for the key/value pair.
			mKeyValuePairSchema = AvroKeyValue
					.getSchema(keySchema, valueSchema);

			// Create an Avro container file and a writer to it.
			DatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<GenericRecord>(
					mKeyValuePairSchema);
			mAvroFileWriter = new DataFileWriter<GenericRecord>(
					genericDatumWriter);
			mAvroFileWriter.setCodec(compressionCodec);
			mAvroFileWriter.setSyncInterval(syncInterval);
			mAvroFileWriter.create(mKeyValuePairSchema, outputStream);

			// Create a reusable output record.
			mOutputRecord = new AvroKeyValue<Object, Object>(
					new GenericData.Record(mKeyValuePairSchema));
		}

		AvroKeyValueWriter(Schema keySchema, Schema valueSchema,
				CodecFactory compressionCodec, OutputStream outputStream)
				throws IOException {
			this(keySchema, valueSchema, compressionCodec, outputStream,
					DataFileConstants.DEFAULT_SYNC_INTERVAL);
		}

		void write(K key, V value) throws IOException {
			mOutputRecord.setKey(key);
			mOutputRecord.setValue(value);
			mAvroFileWriter.append(mOutputRecord.get());
		}

		void close() throws IOException {
			mAvroFileWriter.close();
		}

		long sync() throws IOException {
			return mAvroFileWriter.sync();
		}
	}

	/**
	 * A reusable Avro generic record for writing key/value pairs to the
	 * file.
	 *
	 * <p>taken from AvroKeyValue avro-mapr lib
	 */
	public static class AvroKeyValue<K, V> {
		/** The name of the key value pair generic record. */
		public static final String KEY_VALUE_PAIR_RECORD_NAME = "KeyValuePair";

		/** The namespace of the key value pair generic record. */
		public static final String KEY_VALUE_PAIR_RECORD_NAMESPACE = "org.apache.avro.mapreduce";

		/** The name of the generic record field containing the key. */
		public static final String KEY_FIELD = "key";

		/** The name of the generic record field containing the value. */
		public static final String VALUE_FIELD = "value";

		/** The key/value generic record wrapped by this class. */
		public final GenericRecord mKeyValueRecord;

		/**
		 * Wraps a GenericRecord that is a key value pair.
		 */
		public AvroKeyValue(GenericRecord keyValueRecord) {
			mKeyValueRecord = keyValueRecord;
		}

		public GenericRecord get() {
			return mKeyValueRecord;
		}

		public void setKey(K key) {
			mKeyValueRecord.put(KEY_FIELD, key);
		}

		public void setValue(V value) {
			mKeyValueRecord.put(VALUE_FIELD, value);
		}

		@SuppressWarnings("unchecked")
		public K getKey() {
			return (K) mKeyValueRecord.get(KEY_FIELD);
		}

		@SuppressWarnings("unchecked")
		public V getValue() {
			return (V) mKeyValueRecord.get(VALUE_FIELD);
		}

		/**
		 * Creates a KeyValuePair generic record schema.
		 *
		 * @return A schema for a generic record with two fields: 'key' and
		 *         'value'.
		 */
		public static Schema getSchema(Schema keySchema, Schema valueSchema) {
			Schema schema = Schema.createRecord(KEY_VALUE_PAIR_RECORD_NAME,
					"A key/value pair", KEY_VALUE_PAIR_RECORD_NAMESPACE, false);
			schema.setFields(Arrays.asList(new Schema.Field(KEY_FIELD,
					keySchema, "The key", null), new Schema.Field(VALUE_FIELD,
					valueSchema, "The value", null)));
			return schema;
		}
	}

	Map<String, String> getProperties() {
		return properties;
	}
}
