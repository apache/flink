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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * Deserialization schema that deserializes from Avro binary format using {@link SchemaCoder}.
 *
 * @param <T> type of record it produces
 */
public class RegistryAvroDeserializationSchema<T> extends AvroDeserializationSchema<T> {

	private static final long serialVersionUID = -884738268437806062L;

	/** Provider for schema coder. Used for initializing in each task. */
	private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

	/** Coder used for reading schema from incoming stream. */
	private transient SchemaCoder schemaCoder;

	/**
	 * Creates Avro deserialization schema that reads schema from input stream using provided {@link SchemaCoder}.
	 *
	 * @param recordClazz         class to which deserialize. Should be either
	 *                            {@link SpecificRecord} or {@link GenericRecord}.
	 * @param reader              reader's Avro schema. Should be provided if recordClazz is
	 *                            {@link GenericRecord}
	 * @param schemaCoderProvider schema provider that allows instantiation of {@link SchemaCoder} that will be used for
	 *                            schema reading
	 */
	public RegistryAvroDeserializationSchema(Class<T> recordClazz, @Nullable Schema reader,
			SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
		super(recordClazz, reader);
		this.schemaCoderProvider = schemaCoderProvider;
		this.schemaCoder = schemaCoderProvider.get();
	}

	@Override
	public T deserialize(byte[] message) throws IOException {
			checkAvroInitialized();
			getInputStream().setBuffer(message);
			Schema writerSchema = schemaCoder.readSchema(getInputStream());
			Schema readerSchema = getReaderSchema();

			GenericDatumReader<T> datumReader = getDatumReader();

			datumReader.setSchema(writerSchema);
			datumReader.setExpected(readerSchema);

			return datumReader.read(null, getDecoder());
	}

	@Override
	void checkAvroInitialized() {
		super.checkAvroInitialized();
		if (schemaCoder == null) {
			this.schemaCoder = schemaCoderProvider.get();
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		RegistryAvroDeserializationSchema<?> that = (RegistryAvroDeserializationSchema<?>) o;
		return schemaCoderProvider.equals(that.schemaCoderProvider);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), schemaCoderProvider);
	}
}
