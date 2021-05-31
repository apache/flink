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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroConversions;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

/**
 * Serialization schema that serializes {@link Row} into Avro bytes.
 *
 * <p>Serializes objects that are represented in (nested) Flink rows. It support types that are
 * compatible with Flink's Table & SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime class
 * {@link AvroRowDeserializationSchema} and schema converter {@link AvroSchemaConverter}.
 */
public class AvroRowSerializationSchema implements SerializationSchema<Row> {

    /** Avro record class for serialization. Might be null if record class is not available. */
    private Class<? extends SpecificRecord> recordClazz;

    /** Schema string for deserialization. */
    private String schemaString;

    /** Avro serialization schema. */
    private transient Schema schema;

    /** Writer to serialize Avro record into a byte array. */
    private transient DatumWriter<IndexedRecord> datumWriter;

    /** Output stream to serialize records into byte array. */
    private transient ByteArrayOutputStream arrayOutputStream;

    /** Low-level class for serialization of Avro values. */
    private transient Encoder encoder;

    /**
     * Creates an Avro serialization schema for the given specific record class.
     *
     * @param recordClazz Avro record class used to serialize Flink's row to Avro's record
     */
    public AvroRowSerializationSchema(Class<? extends SpecificRecord> recordClazz) {
        Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
        this.recordClazz = recordClazz;
        this.schema = SpecificData.get().getSchema(recordClazz);
        this.schemaString = schema.toString();
        this.datumWriter = new SpecificDatumWriter<>(schema);
        this.arrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
    }

    /**
     * Creates an Avro serialization schema for the given Avro schema string.
     *
     * @param avroSchemaString Avro schema string used to serialize Flink's row to Avro's record
     */
    public AvroRowSerializationSchema(String avroSchemaString) {
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
        this.recordClazz = null;
        this.schemaString = avroSchemaString;
        try {
            this.schema = new Schema.Parser().parse(avroSchemaString);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Could not parse Avro schema string.", e);
        }
        this.datumWriter = new GenericDatumWriter<>(schema);
        this.arrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
    }

    @Override
    public byte[] serialize(Row row) {
        try {
            // convert to record
            final GenericRecord record = AvroConversions.convertRowToAvroRecord(schema, row);
            arrayOutputStream.reset();
            datumWriter.write(record, encoder);
            encoder.flush();
            return arrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row.", e);
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
        final AvroRowSerializationSchema that = (AvroRowSerializationSchema) o;
        return Objects.equals(recordClazz, that.recordClazz)
                && Objects.equals(schemaString, that.schemaString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordClazz, schemaString);
    }

    // --------------------------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(recordClazz);
        outputStream.writeObject(schemaString); // support for null
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream inputStream)
            throws ClassNotFoundException, IOException {
        recordClazz = (Class<? extends SpecificRecord>) inputStream.readObject();
        schemaString = (String) inputStream.readObject();
        if (recordClazz != null) {
            schema = SpecificData.get().getSchema(recordClazz);
        } else {
            schema = new Schema.Parser().parse(schemaString);
        }
        datumWriter = new SpecificDatumWriter<>(schema);
        arrayOutputStream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
    }
}
