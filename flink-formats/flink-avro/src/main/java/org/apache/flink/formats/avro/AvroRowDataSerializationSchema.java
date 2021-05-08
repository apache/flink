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
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Objects;

/**
 * Serialization schema that serializes {@link RowData} into Avro bytes.
 *
 * <p>Serializes objects that are represented in (nested) Flink RowData. It support types that are
 * compatible with Flink's Table & SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime class
 * {@link AvroRowDataDeserializationSchema} and schema converter {@link AvroSchemaConverter}.
 */
public class AvroRowDataSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** Nested schema to serialize the {@link GenericRecord} into bytes. * */
    private final SerializationSchema<GenericRecord> nestedSchema;

    /** Logical type describing the input type. */
    private final RowType rowType;

    /** Avro serialization schema. */
    private transient Schema schema;

    /** Runtime instance that performs the actual work. */
    private final RowDataToAvroConverters.RowDataToAvroConverter runtimeConverter;

    /** Creates an Avro serialization schema with the given record row type. */
    public AvroRowDataSerializationSchema(RowType rowType) {
        this(
                rowType,
                AvroSerializationSchema.forGeneric(AvroSchemaConverter.convertToSchema(rowType)),
                RowDataToAvroConverters.createConverter(rowType));
    }

    /**
     * Creates an Avro serialization schema with the given record row type, nested schema and
     * runtime converters.
     */
    public AvroRowDataSerializationSchema(
            RowType rowType,
            SerializationSchema<GenericRecord> nestedSchema,
            RowDataToAvroConverters.RowDataToAvroConverter runtimeConverter) {
        this.rowType = rowType;
        this.nestedSchema = nestedSchema;
        this.runtimeConverter = runtimeConverter;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.schema = AvroSchemaConverter.convertToSchema(rowType);
        this.nestedSchema.open(context);
    }

    @Override
    public byte[] serialize(RowData row) {
        try {
            // convert to record
            final GenericRecord record = (GenericRecord) runtimeConverter.convert(schema, row);
            return nestedSchema.serialize(record);
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
        AvroRowDataSerializationSchema that = (AvroRowDataSerializationSchema) o;
        return nestedSchema.equals(that.nestedSchema) && rowType.equals(that.rowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nestedSchema, rowType);
    }
}
