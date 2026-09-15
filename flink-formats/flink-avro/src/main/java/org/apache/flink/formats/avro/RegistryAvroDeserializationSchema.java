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
        if (message == null) {
            return null;
        }
        checkAvroInitialized();
        getInputStream().setBuffer(message);
        Schema writerSchema = schemaCoder.readSchema(getInputStream());
        Schema readerSchema = getReaderSchema();

        GenericDatumReader<T> datumReader = getDatumReader();

        datumReader.setSchema(writerSchema);

        // Create a merged expected schema that:
        // 1. Uses reader schema field structure (correct field order for field projection)
        // 2. But patches in writer schema types where incompatible (e.g., enum instead of string)
        // This combined with name-based field access in AvroToRowDataConverters allows both
        // Debezium-style field projection and enum handling to work correctly.
        Schema expectedSchema =
                readerSchema != null ? mergeSchemaTypes(readerSchema, writerSchema) : writerSchema;

        datumReader.setExpected(expectedSchema);

        if (getEncoding() == AvroEncoding.JSON) {
            ((JsonDecoder) getDecoder()).configure(getInputStream());
        }

        return datumReader.read(null, getDecoder());
    }

    /**
     * Merges reader and writer schemas to create a hybrid schema. Uses reader schema structure
     * (field order) but patches in writer types for incompatible conversions.
     *
     * @param reader the reader schema (from Flink DDL)
     * @param writer the writer schema (from schema registry)
     * @return merged schema with reader structure but writer types where needed
     */
    private Schema mergeSchemaTypes(Schema reader, Schema writer) {
        // If same type, check for specific incompatibilities
        if (reader.getType() == writer.getType()) {
            if (reader.getType() == Schema.Type.RECORD) {
                return mergeRecordSchemas(reader, writer);
            }
            return reader;
        }

        // Handle union types
        if (reader.getType() == Schema.Type.UNION) {
            return mergeUnionSchema(reader, writer);
        }
        if (writer.getType() == Schema.Type.UNION) {
            return mergeUnionSchema(reader, writer);
        }

        // Type mismatch: prefer writer type if it's enum and reader is string
        if (writer.getType() == Schema.Type.ENUM && reader.getType() == Schema.Type.STRING) {
            return writer;
        }

        return reader;
    }

    private Schema mergeRecordSchemas(Schema reader, Schema writer) {
        java.util.List<Schema.Field> mergedFields = new java.util.ArrayList<>();

        for (Schema.Field readerField : reader.getFields()) {
            Schema.Field writerField = writer.getField(readerField.name());

            if (writerField != null) {
                // Field exists in both - merge types
                Schema mergedFieldSchema =
                        mergeSchemaTypes(readerField.schema(), writerField.schema());
                mergedFields.add(
                        new Schema.Field(
                                readerField.name(),
                                mergedFieldSchema,
                                readerField.doc(),
                                readerField.defaultVal()));
            } else {
                // Field only in reader - keep as is
                mergedFields.add(
                        new Schema.Field(
                                readerField.name(),
                                readerField.schema(),
                                readerField.doc(),
                                readerField.defaultVal()));
            }
        }

        Schema merged =
                Schema.createRecord(
                        reader.getName(), reader.getDoc(), reader.getNamespace(), reader.isError());
        merged.setFields(mergedFields);
        return merged;
    }

    private Schema mergeUnionSchema(Schema reader, Schema writer) {
        java.util.List<Schema> readerTypes =
                reader.getType() == Schema.Type.UNION
                        ? reader.getTypes()
                        : java.util.Collections.singletonList(reader);
        java.util.List<Schema> writerTypes =
                writer.getType() == Schema.Type.UNION
                        ? writer.getTypes()
                        : java.util.Collections.singletonList(writer);

        // Find non-null types
        Schema readerNonNull = null;
        boolean readerHasNull = false;
        for (Schema type : readerTypes) {
            if (type.getType() == Schema.Type.NULL) {
                readerHasNull = true;
            } else {
                readerNonNull = type;
            }
        }

        Schema writerNonNull = null;
        for (Schema type : writerTypes) {
            if (type.getType() != Schema.Type.NULL) {
                writerNonNull = type;
            }
        }

        if (readerNonNull != null && writerNonNull != null) {
            Schema mergedNonNull = mergeSchemaTypes(readerNonNull, writerNonNull);
            if (readerHasNull) {
                return Schema.createUnion(Schema.create(Schema.Type.NULL), mergedNonNull);
            }
            return mergedNonNull;
        }

        return reader;
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
