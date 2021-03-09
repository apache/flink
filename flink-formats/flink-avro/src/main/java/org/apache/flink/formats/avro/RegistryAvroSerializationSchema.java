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

import org.apache.flink.util.WrappingRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * Serialization schema that serializes to Avro binary format.
 *
 * @param <T> the type to be serialized
 */
public class RegistryAvroSerializationSchema<T> extends AvroSerializationSchema<T> {

    private static final long serialVersionUID = -6766681879020862312L;

    /** Provider for schema coder. Used for initializing in each task. */
    private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

    protected SchemaCoder schemaCoder;
    /**
     * Creates a Avro serialization schema.
     *
     * @param recordClazz class to serialize. Should be either {@link SpecificRecord} or {@link
     *     GenericRecord}.
     * @param schema writers's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param schemaCoderProvider schema provider that allows instantiation of {@link SchemaCoder}
     *     that will be used for schema writing
     */
    public RegistryAvroSerializationSchema(
            Class<T> recordClazz,
            Schema schema,
            SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(recordClazz, schema);
        this.schemaCoderProvider = schemaCoderProvider;
    }

    public static <T extends SpecificRecord> RegistryAvroSerializationSchema<T> forSpecific(
            Class<T> tClass, SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        return new RegistryAvroSerializationSchema<>(tClass, null, schemaCoderProvider);
    }

    public static RegistryAvroSerializationSchema<GenericRecord> forGeneric(
            Schema schema, SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        return new RegistryAvroSerializationSchema<>(
                GenericRecord.class, schema, schemaCoderProvider);
    }

    @Override
    public byte[] serialize(T object) {
        checkAvroInitialized();

        if (object == null) {
            return null;
        } else {
            try {
                ByteArrayOutputStream outputStream = getOutputStream();
                outputStream.reset();
                Encoder encoder = getEncoder();
                schemaCoder.writeSchema(getSchema(), outputStream);
                getDatumWriter().write(object, encoder);
                encoder.flush();
                return outputStream.toByteArray();
            } catch (IOException e) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
            }
        }
    }

    @Override
    protected void checkAvroInitialized() {
        super.checkAvroInitialized();
        if (schemaCoder == null) {
            schemaCoder = schemaCoderProvider.get();
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
        RegistryAvroSerializationSchema<?> that = (RegistryAvroSerializationSchema<?>) o;
        return schemaCoderProvider.equals(that.schemaCoderProvider);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), schemaCoderProvider);
    }
}
