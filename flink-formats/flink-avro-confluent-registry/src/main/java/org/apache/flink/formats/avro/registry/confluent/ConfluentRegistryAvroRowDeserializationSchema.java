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

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

import static org.apache.flink.formats.avro.utils.AvroDeserializationUtils.convertAvroRecordToRow;

/**
 * Deserialization schema that deserializes from Avro binary format to Row using {@link SchemaCoder} that uses
 * Confluent Schema Registry.
 */
public class ConfluentRegistryAvroRowDeserializationSchema extends AbstractDeserializationSchema<Row> {
	public static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

	@VisibleForTesting
	String schemaString;

	@VisibleForTesting
	String url;

	@VisibleForTesting
	Class<? extends SpecificRecord> recordClazz;

	private int identityMapCapacity;

	private transient Schema schema;

	private transient RowTypeInfo typeInfo;

	private transient IndexedRecord record;

	private transient GenericDatumReader<IndexedRecord> datumReader;

	private transient MutableByteArrayInputStream inputStream;

	private transient Decoder decoder;

	@VisibleForTesting
	transient SchemaCoder schemaCoder;

	@VisibleForTesting
	ConfluentRegistryAvroRowDeserializationSchema() {}

	/**
	 * Creates {@link AbstractDeserializationSchema} that produces {@link Row}
	 * using provided reader schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param schema 				schema of produced records
	 * @param url   				url of schema registry to connect
	 * @param identityMapCapacity	maximum number of cached schema versions (default: 1000)
	 * @return deserialized record in form of {@link Row}
	 */
	public static ConfluentRegistryAvroRowDeserializationSchema forGeneric(Schema schema, String url,
			int identityMapCapacity) {
		return new ConfluentRegistryAvroRowDeserializationSchema(schema, url, identityMapCapacity);
	}

	/**
	 * Creates {@link AbstractDeserializationSchema} that produces {@link Row}
	 * using reader provided reader schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param schema schema of produced records
	 * @param url    url of schema registry to connect
	 * @return deserialized record in form of {@link Row}
	 */
	public static ConfluentRegistryAvroRowDeserializationSchema forGeneric(Schema schema, String url) {
		return forGeneric(schema, url, DEFAULT_IDENTITY_MAP_CAPACITY);
	}

	/**
	 * Creates {@link AbstractDeserializationSchema} that produces {@link Row}
	 * using reader schema provided by {@link SpecificRecord} and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param recordClazz 			specific record class
	 * @param url    				url of schema registry to connect
	 * @param identityMapCapacity	maximum number of cached schema versions (default: 1000)
	 * @return deserialized record in form of {@link Row}
	 */
	public static ConfluentRegistryAvroRowDeserializationSchema forSpecific(Class<? extends SpecificRecord> recordClazz,
			String url, int identityMapCapacity) {
		return new ConfluentRegistryAvroRowDeserializationSchema(recordClazz, url, identityMapCapacity);
	}

	/**
	 * Creates {@link AbstractDeserializationSchema} that produces {@link Row}
	 * using schema provided by {@link SpecificRecord} and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param recordClazz 			specific record class
	 * @param url    				url of schema registry to connect
	 * @return deserialized record in form of {@link Row}
	 */
	public static ConfluentRegistryAvroRowDeserializationSchema forSpecific(Class<? extends SpecificRecord> recordClazz,
			String url) {
		return forSpecific(recordClazz, url, DEFAULT_IDENTITY_MAP_CAPACITY);
	}

	protected ConfluentRegistryAvroRowDeserializationSchema(Schema schema, String url, int identityMapCapacity) {
		this.url = url;
		this.identityMapCapacity = identityMapCapacity;
		this.schema = schema;
		this.recordClazz = null;

		schemaString = schema.toString();
		typeInfo = getTypeInfo(schemaString);
		schemaCoder = new ConfluenceCachedSchemaCoderProvider(url, identityMapCapacity).get();
		record = new GenericData.Record(schema);
		datumReader = new GenericDatumReader<>(schema);
		inputStream = new MutableByteArrayInputStream();
		decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
	}

	protected ConfluentRegistryAvroRowDeserializationSchema(Class<? extends SpecificRecord> recordClazz, String url,
			int identityMapCapacity) {
		this.url = url;
		this.identityMapCapacity = identityMapCapacity;
		this.recordClazz = recordClazz;
		this.schema = SpecificData.get().getSchema(recordClazz);

		schemaString = schema.toString();
		typeInfo = getTypeInfo(schemaString);
		schemaCoder = new ConfluenceCachedSchemaCoderProvider(url, identityMapCapacity).get();
		record = new GenericData.Record(schema);
		datumReader = new GenericDatumReader<>(schema);
		inputStream = new MutableByteArrayInputStream();
		decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
	}

	private RowTypeInfo getTypeInfo(String schemaString) {
		TypeInformation<?> typeInfo = AvroSchemaConverter.convertToTypeInfo(schemaString);
		Preconditions.checkArgument(typeInfo instanceof RowTypeInfo, "Row type information expected.");
		return (RowTypeInfo) typeInfo;
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			inputStream.setBuffer(message);
			Schema writerSchema = schemaCoder.readSchema(inputStream);

			datumReader.setSchema(writerSchema);
			datumReader.setExpected(schema);

			record = datumReader.read(record, decoder);
			return convertAvroRecordToRow(schema, typeInfo, record);
		} catch (Exception e) {
			throw new IOException("Failed to deserialize Avro record.", e);
		}
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	@VisibleForTesting
	void writeObject(ObjectOutputStream outputStream) throws IOException {
		outputStream.writeObject(recordClazz);
		outputStream.writeUTF(schemaString);
		outputStream.writeUTF(url);
		outputStream.writeInt(identityMapCapacity);
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
		recordClazz = (Class<? extends SpecificRecord>) inputStream.readObject();
		schemaString = inputStream.readUTF();
		url = inputStream.readUTF();
		identityMapCapacity = inputStream.readInt();

		typeInfo = (RowTypeInfo) AvroSchemaConverter.<Row>convertToTypeInfo(schemaString);
		schema = new Schema.Parser().parse(schemaString);
		if (recordClazz != null) {
			record = (SpecificRecord) SpecificData.newInstance(recordClazz, schema);
		} else {
			record = new GenericData.Record(schema);
		}

		datumReader = new SpecificDatumReader<>(schema);
		this.inputStream = new MutableByteArrayInputStream();
		decoder = DecoderFactory.get().binaryDecoder(this.inputStream, null);

		schemaCoder = new ConfluenceCachedSchemaCoderProvider(url, identityMapCapacity).get();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ConfluentRegistryAvroRowDeserializationSchema that = (ConfluentRegistryAvroRowDeserializationSchema) o;
		return Objects.equals(schemaString, that.schemaString) &&
			Objects.equals(url, that.url) &&
			Objects.equals(recordClazz, that.recordClazz);
	}

	@Override
	public int hashCode() {
		return Objects.hash(schemaString, url, recordClazz);
	}
}
