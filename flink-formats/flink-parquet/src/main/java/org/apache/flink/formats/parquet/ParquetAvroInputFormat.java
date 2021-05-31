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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.AvroConversions;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link ParquetInputFormat} to read records from Parquet files and convert
 * them to Avro GenericRecord. To use it the user needs to add {@code flink-avro} optional
 * dependency to the classpath. Usage:
 *
 * <pre>{@code
 * final ParquetAvroInputFormat inputFormat = new ParquetAvroInputFormat(new Path(filePath), parquetSchema);
 * DataSource<GenericRecord> source = env.createInput(inputFormat, new GenericRecordAvroTypeInfo(inputFormat.getAvroSchema()));
 *
 * }</pre>
 */
public class ParquetAvroInputFormat extends ParquetInputFormat<GenericRecord>
        implements ResultTypeQueryable<GenericRecord> {

    private static final long serialVersionUID = 1L;

    private transient Schema avroSchema;
    private String avroSchemaString;

    public ParquetAvroInputFormat(Path filePath, MessageType messageType) {
        super(filePath, messageType);
        avroSchema = new AvroSchemaConverter().convert(messageType);
        avroSchemaString = avroSchema.toString();
    }

    @Override
    public void selectFields(String[] fieldNames) {
        avroSchema = getProjectedSchema(fieldNames, avroSchema);
        avroSchemaString = avroSchema.toString();
        super.selectFields(fieldNames);
    }

    @Override
    protected GenericRecord convert(Row row) {
        // after deserialization
        if (avroSchema == null) {
            avroSchema = new Schema.Parser().parse(avroSchemaString);
        }
        return AvroConversions.convertRowToAvroRecord(avroSchema, row);
    }

    @Override
    public GenericRecordAvroTypeInfo getProducedType() {
        return new GenericRecordAvroTypeInfo(avroSchema);
    }

    private Schema getProjectedSchema(String[] projectedFieldNames, Schema sourceAvroSchema) {
        // Avro fields need to be in the same order than row field for compatibility with flink 1.12
        // (row
        // fields not accessible by name). Row field order now is the order of
        // ParquetInputFormat.selectFields()
        // for flink 1.13+ where row fields are accessible by name, we will match the fields names
        // between avro schema and row schema.

        List<Schema.Field> projectedFields = new ArrayList<>();
        for (String fieldName : projectedFieldNames) {
            projectedFields.add(deepCopyField(sourceAvroSchema.getField(fieldName)));
        }
        return Schema.createRecord(
                sourceAvroSchema.getName() + "_projected",
                sourceAvroSchema.getDoc(),
                sourceAvroSchema.getNamespace(),
                sourceAvroSchema.isError(),
                projectedFields);
    }

    private Schema.Field deepCopyField(Schema.Field field) {
        Schema.Field newField =
                new Schema.Field(
                        field.name(),
                        field.schema(),
                        field.doc(),
                        field.defaultVal(),
                        field.order());
        for (Map.Entry<String, Object> kv : field.getObjectProps().entrySet()) {
            newField.addProp(kv.getKey(), kv.getValue());
        }
        if (field.aliases() != null) {
            for (String alias : field.aliases()) {
                newField.addAlias(alias);
            }
        }
        return newField;
    }

    public Schema getAvroSchema() {
        return avroSchema;
    }
}
