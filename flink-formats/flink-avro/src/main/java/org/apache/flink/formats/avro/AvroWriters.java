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

package org.apache.flink.formats.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Convenience builder to create {@link AvroWriterFactory} instances for the different Avro types.
 */
public class AvroWriters {

	/**
	 * A configurator to set the properties of the writer.
	 */
	public interface WriterConfigurator<T> extends Serializable {

		/**
		 * Modifies the properties of the writer.
		 *
		 * @param dataFileWriter The writer to modify.
		 */
		void configureWriter(DataFileWriter<T> dataFileWriter);

	}

	/**
	 * Creates an {@link AvroWriterFactory} for an Avro specific type. The Avro writers
	 * will use the schema of that specific type to build and write the records.
	 *
	 * @param type The class of the type to write.
	 */
	public static <T extends SpecificRecordBase> AvroWriterFactory<T> forSpecificRecord(Class<T> type) {
		return forSpecificRecord(type, writer -> {});
	}

	/**
	 * Creates an {@link AvroWriterFactory} for an Avro specific type and given <tt>configurator</tt>.
	 * The Avro writers will use the schema of that specific type to build and write the records.
	 *
	 * @param type The class of the type to write.
	 * @param configurator The configurator to modify the writer properties.
	 */
	public static <T extends SpecificRecordBase> AvroWriterFactory<T> forSpecificRecord(
		Class<T> type,
		WriterConfigurator<T> configurator) {

		String schemaString = SpecificData.get().getSchema(type).toString();
		AvroBuilder<T> builder = (out) -> createAvroDataFileWriter(
			schemaString,
			AvroRecordType.SPECIFIC,
			configurator,
			out);
		return new AvroWriterFactory<>(builder);
	}

	/**
	 * Creates an {@link AvroWriterFactory} that accepts and writes Avro generic types.
	 * The Avro writers will use the given schema to build and write the records.
	 *
	 * @param schema The schema of the generic type.
	 */
	public static AvroWriterFactory<GenericRecord> forGenericRecord(Schema schema) {
		return forGenericRecord(schema, writer -> {});
	}

	/**
	 * Creates an {@link AvroWriterFactory} that accepts and writes Avro generic types
	 * and given <tt>configurator</tt>. The Avro writers will use the given schema to
	 * build and write the records.
	 *
	 * @param schema The schema of the generic type.
	 * @param configurator The configurator to modify the writer properties.
	 */
	public static AvroWriterFactory<GenericRecord> forGenericRecord(
		Schema schema,
		WriterConfigurator<GenericRecord> configurator) {

		String schemaString = schema.toString();
		AvroBuilder<GenericRecord> builder = (out) -> createAvroDataFileWriter(
			schemaString,
			AvroRecordType.GENERIC,
			configurator,
			out);
		return new AvroWriterFactory<>(builder);
	}

	/**
	 * Creates an {@link AvroWriterFactory} for the given type. The Avro writers will
	 * use reflection to create the schema for the type and use that schema to write
	 * the records.
	 *
	 * @param type The class of the type to write.
	 */
	public static <T> AvroWriterFactory<T> forReflectRecord(Class<T> type) {
		return forReflectRecord(type, writer -> {});
	}

	/**
	 * Creates an {@link AvroWriterFactory} for the given type and given <tt>configurator</tt>.
	 * The Avro writers will use reflection to create the schema for the type and use that schema
	 * to write the records.
	 *
	 * @param type The class of the type to write.
	 * @param configurator The configurator to modify the writer properties.
	 */
	public static <T> AvroWriterFactory<T> forReflectRecord(
		Class<T> type,
		WriterConfigurator<T> configurator) {

		String schemaString = ReflectData.get().getSchema(type).toString();
		AvroBuilder<T> builder = (out) -> createAvroDataFileWriter(
			schemaString,
			AvroRecordType.REFLECT,
			configurator,
			out);
		return new AvroWriterFactory<>(builder);
	}

	private static <T> DataFileWriter<T> createAvroDataFileWriter(
		String schemaString,
		AvroRecordType recordType,
		WriterConfigurator<T> configurator,
		OutputStream out) throws IOException {

		Schema schema = new Schema.Parser().parse(schemaString);
		DatumWriter<T> datumWriter = null;

		switch (recordType) {
			case SPECIFIC:
				datumWriter = new SpecificDatumWriter<>(schema);
				break;
			case GENERIC:
				datumWriter = new GenericDatumWriter<>(schema);
				break;
			case REFLECT:
				datumWriter = new ReflectDatumWriter<>(schema);
				break;
		}

		DataFileWriter<T> dataFileWriter = new DataFileWriter<>(datumWriter);
		configurator.configureWriter(dataFileWriter);
		dataFileWriter.create(schema, out);
		return dataFileWriter;
	}

	// ------------------------------------------------------------------------

	/**
	 * The type of the avro record.
	 */
	private enum AvroRecordType {
		/**
		 * The record type is subclass of {@link SpecificRecord}, which is usually generated
		 * from .avro files.
		 */
		SPECIFIC,

		/** The record uses {@link GenericRecord} directly. */
		GENERIC,

		/** The record is of arbitrary type and its schema is deducted with reflection. */
		REFLECT
	}

	/** Class is not meant to be instantiated. */
	private AvroWriters() {}
}
