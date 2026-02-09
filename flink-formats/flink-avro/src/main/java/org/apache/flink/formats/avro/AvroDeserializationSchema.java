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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.formats.avro.typeutils.AvroFactory;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nullable;

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
        return forGeneric(schema, AvroEncoding.BINARY);
    }

    /**
     * Creates {@link AvroDeserializationSchema} that produces {@link GenericRecord} using provided
     * schema.
     *
     * @param schema schema of produced records
     * @param encoding Avro serialization approach to use for decoding
     * @return deserialized record in form of {@link GenericRecord}
     */
    public static AvroDeserializationSchema<GenericRecord> forGeneric(
            Schema schema, AvroEncoding encoding) {
        return forGeneric(null, schema, encoding, null, false);
    }

    /**
     * Creates {@link AvroDeserializationSchema} that produces {@link GenericRecord} using provided
     * schema.
     *
     * @param actualSchema schema of upstream actual write
     * @param schema schema of produced records
     * @param encoding Avro serialization approach to use for decoding
     * @param actualSchemaString Avro writer's Schema.
     * @param fastRead option to enable or disable fastread.
     * @return deserialized record in form of {@link GenericRecord}
     */
    public static AvroDeserializationSchema<GenericRecord> forGeneric(
            Schema actualSchema,
            Schema schema,
            AvroEncoding encoding,
            String actualSchemaString,
            boolean fastRead) {
        return new AvroDeserializationSchema<>(
                GenericRecord.class, schema, actualSchema, encoding, actualSchemaString, fastRead);
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
        return forSpecific(tClass, AvroEncoding.BINARY);
    }

    /**
     * Creates {@link AvroDeserializationSchema} that produces classes that were generated from avro
     * schema.
     *
     * @param tClass class of record to be produced
     * @param encoding Avro serialization approach to use for decoding
     * @return deserialized record
     */
    public static <T extends SpecificRecord> AvroDeserializationSchema<T> forSpecific(
            Class<T> tClass, AvroEncoding encoding) {
        return new AvroDeserializationSchema<>(tClass, null, encoding);
    }

    private static final long serialVersionUID = -6766681879020862312L;

    /** Class to deserialize to. */
    private final Class<T> recordClazz;

    /** Schema in case of GenericRecord for downstream serialization purpose. */
    private final String schemaString;

    /** Schema in case of GenericRecord for upstream serialization purpose. */
    private final String writeSchemaString;

    /** Reader that deserializes byte array into a record. */
    private transient GenericDatumReader<T> datumReader;

    /** Input stream to read message from. */
    private transient MutableByteArrayInputStream inputStream;

    /** Config option for the deserialization approach to use. */
    private final AvroEncoding encoding;

    /** Avro decoder that decodes data. */
    private transient Decoder decoder;

    /** Avro schema for the downstream reader. */
    private transient Schema reader;

    /** Avro schema for the upstream writer. */
    private transient Schema writer;

    /** Avro schema for the upstream writer. */
    private final boolean fastRead;

    /**
     * Creates a Avro deserialization schema.
     *
     * @param recordClazz class to which deserialize. Should be one of: {@link
     *     org.apache.avro.specific.SpecificRecord}, {@link org.apache.avro.generic.GenericRecord}.
     * @param reader reader's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param encoding encoding approach to use. Identifies the Avro decoder class to use.
     */
    AvroDeserializationSchema(
            Class<T> recordClazz, @Nullable Schema reader, AvroEncoding encoding) {
        this(recordClazz, reader, null, encoding, null, false);
    }

    /**
     * Creates a Avro deserialization schema.
     *
     * @param recordClazz class to which deserialize. Should be one of: {@link
     *     org.apache.avro.specific.SpecificRecord}, {@link org.apache.avro.generic.GenericRecord}.
     * @param reader reader's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param writer writer's Avro schema. Should be provided if user want to column pruning.
     * @param encoding encoding approach to use. Identifies the Avro decoder class to use.
     * @param writeSchemaString Optional for advanced users manually specify.
     * @param fastRead option to enable or disable fastread.
     */
    AvroDeserializationSchema(
            Class<T> recordClazz,
            @Nullable Schema reader,
            @Nullable Schema writer,
            AvroEncoding encoding,
            @Nullable String writeSchemaString,
            boolean fastRead) {
        Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
        this.recordClazz = recordClazz;
        this.reader = reader;
        if (reader != null) {
            this.schemaString = reader.toString();
        } else {
            this.schemaString = null;
        }
        this.writer = writer;
        if (writeSchemaString != null) {
            this.writeSchemaString = writeSchemaString;
        } else if (this.writer != null) {
            this.writeSchemaString = this.writer.toString();
        } else {
            this.writeSchemaString = null;
        }
        this.encoding = encoding;
        this.fastRead = fastRead;
    }

    GenericDatumReader<T> getDatumReader() {
        return datumReader;
    }

    Schema getReaderSchema() {
        return writer != null ? writer : reader;
    }

    MutableByteArrayInputStream getInputStream() {
        return inputStream;
    }

    Decoder getDecoder() {
        return decoder;
    }

    AvroEncoding getEncoding() {
        return encoding;
    }

    @Override
    public T deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        // read record
        checkAvroInitialized();
        inputStream.setBuffer(message);
        if (encoding == AvroEncoding.JSON) {
            ((JsonDecoder) this.decoder).configure(inputStream);
        }
        return datumReader.read(null, decoder);
    }

    void checkAvroInitialized() throws IOException {
        if (datumReader != null) {
            return;
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
            @SuppressWarnings("unchecked")
            SpecificData specificData =
                    AvroFactory.getSpecificDataForClass(
                            (Class<? extends SpecificData>) recordClazz, cl);
            specificData.setFastReaderEnabled(fastRead);
            this.datumReader = new SpecificDatumReader<>(specificData);
            this.reader = AvroFactory.extractAvroSpecificSchema(recordClazz, specificData);
            datumReader.setSchema(reader);
        } else {
            this.reader = new Schema.Parser().parse(schemaString);
            GenericData genericData = new GenericData(cl);
            genericData.setFastReaderEnabled(fastRead);
            this.writer =
                    new Schema.Parser()
                            .parse(
                                    writeSchemaString == null || writeSchemaString.isEmpty()
                                            ? schemaString
                                            : writeSchemaString);
            this.datumReader = new GenericDatumReader<>(this.writer, this.reader, genericData);
        }

        this.inputStream = new MutableByteArrayInputStream();

        if (encoding == AvroEncoding.JSON) {
            this.decoder = DecoderFactory.get().jsonDecoder(getReaderSchema(), inputStream);
        } else {
            this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        }
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
        return fastRead == that.fastRead
                && recordClazz.equals(that.recordClazz)
                && Objects.equals(schemaString, that.schemaString)
                && Objects.equals(
                        getEffectiveWriteSchemaString(), that.getEffectiveWriteSchemaString())
                && encoding == that.encoding;
    }

    private String getEffectiveWriteSchemaString() {
        return (writeSchemaString == null || writeSchemaString.isEmpty())
                ? schemaString
                : writeSchemaString;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                recordClazz, schemaString, getEffectiveWriteSchemaString(), encoding, fastRead);
    }
}
