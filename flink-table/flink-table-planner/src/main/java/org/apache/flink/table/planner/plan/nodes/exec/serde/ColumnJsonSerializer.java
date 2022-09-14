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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Column;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.serializeOptionalField;

/**
 * JSON serializer for {@link Column}.
 *
 * @see ColumnJsonDeserializer for the reverse operation
 */
@Internal
final class ColumnJsonSerializer extends StdSerializer<Column> {

    static final String KIND = "kind";
    static final String KIND_PHYSICAL = "PHYSICAL";
    static final String KIND_COMPUTED = "COMPUTED";
    static final String KIND_METADATA = "METADATA";
    static final String NAME = "name";
    static final String DATA_TYPE = "dataType";
    static final String COMMENT = "comment";
    static final String EXPRESSION = "expression";
    static final String METADATA_KEY = "metadataKey";
    static final String IS_VIRTUAL = "isVirtual";

    ColumnJsonSerializer() {
        super(Column.class);
    }

    @Override
    public void serialize(
            Column column, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        // Common fields
        jsonGenerator.writeStringField(NAME, column.getName());
        serializeOptionalField(jsonGenerator, COMMENT, column.getComment(), serializerProvider);

        if (column instanceof Column.PhysicalColumn) {
            serialize((Column.PhysicalColumn) column, jsonGenerator, serializerProvider);
        } else if (column instanceof Column.MetadataColumn) {
            serialize((Column.MetadataColumn) column, jsonGenerator, serializerProvider);
        } else if (column instanceof Column.ComputedColumn) {
            serialize((Column.ComputedColumn) column, jsonGenerator, serializerProvider);
        }

        jsonGenerator.writeEndObject();
    }

    private void serialize(
            Column.PhysicalColumn column,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        serializerProvider.defaultSerializeField(DATA_TYPE, column.getDataType(), jsonGenerator);
    }

    private void serialize(
            Column.MetadataColumn column,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStringField(KIND, KIND_METADATA);
        serializerProvider.defaultSerializeField(DATA_TYPE, column.getDataType(), jsonGenerator);
        serializeOptionalField(
                jsonGenerator, METADATA_KEY, column.getMetadataKey(), serializerProvider);
        jsonGenerator.writeBooleanField(IS_VIRTUAL, column.isVirtual());
    }

    private void serialize(
            Column.ComputedColumn column,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStringField(KIND, KIND_COMPUTED);
        serializerProvider.defaultSerializeField(EXPRESSION, column.getExpression(), jsonGenerator);
    }
}
