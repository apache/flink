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

import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Deserialization schema that deserializes from Avro format using {@link SchemaCoder}.
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
     * Creates Avro deserialization schema that reads schema from input stream using provided {@link
     * SchemaCoder}.
     *
     * @param recordClazz class to which deserialize. Should be either {@link SpecificRecord} or
     *     {@link GenericRecord}.
     * @param reader reader's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param schemaCoderProvider schema provider that allows instantiation of {@link SchemaCoder}
     *     that will be used for schema reading
     */
    public RegistryAvroDeserializationSchema(
            Class<T> recordClazz,
            @Nullable Schema reader,
            SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        this(recordClazz, reader, schemaCoderProvider, AvroEncoding.BINARY);
    }

    /**
     * Creates Avro deserialization schema that reads schema from input stream using provided {@link
     * SchemaCoder}.
     *
     * @param recordClazz class to which deserialize. Should be either {@link SpecificRecord} or
     *     {@link GenericRecord}.
     * @param reader reader's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param schemaCoderProvider schema provider that allows instantiation of {@link SchemaCoder}
     *     that will be used for schema reading
     * @param encoding Avro serialization approach to use. Required to identify the correct decoder
     *     class to use.
     */
    public RegistryAvroDeserializationSchema(
            Class<T> recordClazz,
            @Nullable Schema reader,
            SchemaCoder.SchemaCoderProvider schemaCoderProvider,
            AvroEncoding encoding) {
        super(recordClazz, reader, encoding);
        this.schemaCoderProvider = schemaCoderProvider;
        this.schemaCoder = schemaCoderProvider.get();
    }

    @Override
    public T deserialize(@Nullable byte[] message) throws IOException {
        return deserializeWithHeaders(message, new HashMap<>());
    }

    @Override
    public T deserializeWithHeaders(byte[] message, Map<String, Object> headers)
            throws IOException {
        if (message == null) {
            return null;
        }
        checkAvroInitialized();
        getInputStream().setBuffer(message);
        // get the schema passing headers
        Schema writerSchema = schemaCoder.readSchemaWithHeaders(getInputStream(), headers);

        Schema readerSchema = getReaderSchema();

        GenericDatumReader<T> datumReader = getDatumReader();

        datumReader.setSchema(writerSchema);
        datumReader.setExpected(readerSchema);

        if (getEncoding() == AvroEncoding.JSON) {
            ((JsonDecoder) getDecoder()).configure(getInputStream());
        }

        return datumReader.read(null, getDecoder());
    }

    @Override
    void checkAvroInitialized() throws IOException {
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
