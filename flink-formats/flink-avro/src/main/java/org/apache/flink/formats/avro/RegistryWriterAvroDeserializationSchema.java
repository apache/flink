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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * Deserialization schema that deserializes from Avro binary format using a {@link SchemaCoder} to
 * resolve the writer schema dynamically per record. Unlike {@link
 * RegistryAvroDeserializationSchema} which projects records into a fixed reader schema, this class
 * uses the writer schema as both reader and writer — preserving all fields as-is.
 *
 * <p>Designed for use cases where the Avro schema is not known at table creation time and varies
 * per record.
 */
public class RegistryWriterAvroDeserializationSchema
        implements DeserializationSchema<GenericRecord> {

    private static final long serialVersionUID = 1L;

    private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

    private transient SchemaCoder schemaCoder;
    private transient MutableByteArrayInputStream inputStream;
    private transient BinaryDecoder decoder;
    private transient GenericDatumReader<GenericRecord> datumReader;

    public RegistryWriterAvroDeserializationSchema(
            SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        this.schemaCoderProvider = Objects.requireNonNull(schemaCoderProvider);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        GenericData genericData = new GenericData(cl);
        this.datumReader = new GenericDatumReader<>(null, null, genericData);
        this.inputStream = new MutableByteArrayInputStream();
        this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        this.schemaCoder = schemaCoderProvider.get();
    }

    @Override
    public GenericRecord deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }

        inputStream.setBuffer(message);
        Schema writerSchema = schemaCoder.readSchema(inputStream);
        datumReader.setSchema(writerSchema);
        datumReader.setExpected(writerSchema);

        return datumReader.read(null, decoder);
    }

    @Override
    public boolean isEndOfStream(GenericRecord nextElement) {
        return false;
    }

    // This is unused as this is composed in AvroVariantDeserializationSchema
    // which has its own type info.
    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return TypeInformation.of(GenericRecord.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RegistryWriterAvroDeserializationSchema that = (RegistryWriterAvroDeserializationSchema) o;
        return schemaCoderProvider.equals(that.schemaCoderProvider);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaCoderProvider);
    }
}
