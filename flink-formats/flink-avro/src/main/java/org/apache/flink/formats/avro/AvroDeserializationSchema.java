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
import org.apache.flink.formats.avro.typeutils.AvroFactory;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * Deserialization schema that deserializes from Avro binary format.
 *
 * @param <T> type of record it produces
 */
public class AvroDeserializationSchema<T> implements DeserializationSchema<T> {

    /**
     * Creates {@link AvroDeserializationSchema} that produces {@link GenericRecord} using provided
     * schema.
     *
     * @param schema schema of produced records
     * @return deserialized record in form of {@link GenericRecord}
     */
    public static AvroDeserializationSchema<GenericRecord> forGeneric(Schema schema) {
        return new AvroDeserializationSchema<>(GenericRecord.class, schema);
    }

    /**
     * Creates {@link AvroDeserializationSchema} that produces classes that were generated from avro
     * schema.
     *
     * @param tClass class of record to be produced
     * @return deserialized record
     */
    public static <T extends SpecificRecord> AvroDeserializationSchema<T> forSpecific(
            Class<T> tClass) {
        return new AvroDeserializationSchema<>(tClass, null);
    }

    private static final long serialVersionUID = -6766681879020862312L;

    /** Class to deserialize to. */
    private final Class<T> recordClazz;

    /** Schema in case of GenericRecord for serialization purpose. */
    private final String schemaString;

    /** Reader that deserializes byte array into a record. */
    private transient GenericDatumReader<T> datumReader;

    /** Input stream to read message from. */
    private transient MutableByteArrayInputStream inputStream;

    /** Avro decoder that decodes binary data. */
    private transient Decoder decoder;

    /** Avro schema for the reader. */
    private transient Schema reader;

    /**
     * Creates a Avro deserialization schema.
     *
     * @param recordClazz class to which deserialize. Should be one of: {@link
     *     org.apache.avro.specific.SpecificRecord}, {@link org.apache.avro.generic.GenericRecord}.
     * @param reader reader's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     */
    AvroDeserializationSchema(Class<T> recordClazz, @Nullable Schema reader) {
        Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
        this.recordClazz = recordClazz;
        this.reader = reader;
        if (reader != null) {
            this.schemaString = reader.toString();
        } else {
            this.schemaString = null;
        }
    }

    GenericDatumReader<T> getDatumReader() {
        return datumReader;
    }

    Schema getReaderSchema() {
        return reader;
    }

    MutableByteArrayInputStream getInputStream() {
        return inputStream;
    }

    Decoder getDecoder() {
        return decoder;
    }

    @Override
    public T deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        // read record
        checkAvroInitialized();
        inputStream.setBuffer(message);
        Schema readerSchema = getReaderSchema();
        GenericDatumReader<T> datumReader = getDatumReader();

        datumReader.setSchema(readerSchema);

        return datumReader.read(null, decoder);
    }

    void checkAvroInitialized() {
        if (datumReader != null) {
            return;
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
            @SuppressWarnings("unchecked")
            SpecificData specificData =
                    AvroFactory.getSpecificDataForClass(
                            (Class<? extends SpecificData>) recordClazz, cl);
            this.datumReader = new SpecificDatumReader<>(specificData);
            this.reader = AvroFactory.extractAvroSpecificSchema(recordClazz, specificData);
        } else {
            this.reader = new Schema.Parser().parse(schemaString);
            GenericData genericData = new GenericData(cl);
            this.datumReader = new GenericDatumReader<>(null, this.reader, genericData);
        }

        this.inputStream = new MutableByteArrayInputStream();
        this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeInformation<T> getProducedType() {
        if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
            return new AvroTypeInfo(recordClazz);
        } else {
            return (TypeInformation<T>) new GenericRecordAvroTypeInfo(this.reader);
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
        AvroDeserializationSchema<?> that = (AvroDeserializationSchema<?>) o;
        return recordClazz.equals(that.recordClazz) && Objects.equals(reader, that.reader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordClazz, reader);
    }
}
