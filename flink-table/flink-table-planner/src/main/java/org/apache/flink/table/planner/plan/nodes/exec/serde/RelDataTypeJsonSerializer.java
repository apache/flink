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

import org.apache.flink.table.planner.plan.schema.GenericRelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;
import org.apache.flink.table.planner.plan.schema.StructuredRelDataType;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;

/**
 * JSON serializer for {@link RelDataType}. refer to {@link RelDataTypeJsonDeserializer} for
 * deserializer.
 */
public class RelDataTypeJsonSerializer extends StdSerializer<RelDataType> {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_TYPE_NAME = "typeName";
    public static final String FIELD_NAME_FILED_NAME = "fieldName";
    public static final String FIELD_NAME_NULLABLE = "nullable";
    public static final String FIELD_NAME_PRECISION = "precision";
    public static final String FIELD_NAME_SCALE = "scale";
    public static final String FIELD_NAME_FIELDS = "fields";
    public static final String FIELD_NAME_STRUCT_KIND = "structKind";
    public static final String FIELD_NAME_TIMESTAMP_KIND = "timestampKind";
    public static final String FIELD_NAME_ELEMENT = "element";
    public static final String FIELD_NAME_KEY = "key";
    public static final String FIELD_NAME_VALUE = "value";
    public static final String FIELD_NAME_TYPE_INFO = "typeInfo";
    public static final String FIELD_NAME_RAW_TYPE = "rawType";
    public static final String FIELD_NAME_STRUCTURED_TYPE = "structuredType";

    public RelDataTypeJsonSerializer() {
        super(RelDataType.class);
    }

    @Override
    public void serialize(
            RelDataType relDataType,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        serialize(relDataType, jsonGenerator);
        jsonGenerator.writeEndObject();
    }

    private void serialize(RelDataType relDataType, JsonGenerator gen) throws IOException {
        if (relDataType instanceof TimeIndicatorRelDataType) {
            TimeIndicatorRelDataType timeIndicatorType = (TimeIndicatorRelDataType) relDataType;
            gen.writeStringField(
                    FIELD_NAME_TIMESTAMP_KIND,
                    timeIndicatorType.isEventTime()
                            ? TimestampKind.ROWTIME.name()
                            : TimestampKind.PROCTIME.name());
            gen.writeStringField(
                    FIELD_NAME_TYPE_NAME, timeIndicatorType.originalType().getSqlTypeName().name());
            gen.writeBooleanField(FIELD_NAME_NULLABLE, relDataType.isNullable());
        } else if (relDataType instanceof StructuredRelDataType) {
            StructuredRelDataType structuredType = (StructuredRelDataType) relDataType;
            gen.writeObjectField(FIELD_NAME_STRUCTURED_TYPE, structuredType.getStructuredType());
        } else if (relDataType.isStruct()) {
            gen.writeStringField(FIELD_NAME_STRUCT_KIND, relDataType.getStructKind().name());
            gen.writeBooleanField(FIELD_NAME_NULLABLE, relDataType.isNullable());

            gen.writeFieldName(FIELD_NAME_FIELDS);
            gen.writeStartArray();
            for (RelDataTypeField field : relDataType.getFieldList()) {
                gen.writeStartObject();
                serialize(field.getType(), gen);
                gen.writeStringField(FIELD_NAME_FILED_NAME, field.getName());
                gen.writeEndObject();
            }
            gen.writeEndArray();
        } else if (relDataType.getSqlTypeName() == SqlTypeName.ARRAY) {
            serializeCommon(relDataType, gen);
            ArraySqlType arraySqlType = (ArraySqlType) relDataType;

            gen.writeFieldName(FIELD_NAME_ELEMENT);
            gen.writeStartObject();
            serialize(arraySqlType.getComponentType(), gen);
            gen.writeEndObject();
        } else if (relDataType.getSqlTypeName() == SqlTypeName.MULTISET) {
            assert relDataType instanceof MultisetSqlType;
            serializeCommon(relDataType, gen);
            MultisetSqlType multisetSqlType = (MultisetSqlType) relDataType;

            gen.writeFieldName(FIELD_NAME_ELEMENT);
            gen.writeStartObject();
            serialize(multisetSqlType.getComponentType(), gen);
            gen.writeEndObject();
        } else if (relDataType.getSqlTypeName() == SqlTypeName.MAP) {
            assert relDataType instanceof MapSqlType;
            serializeCommon(relDataType, gen);
            MapSqlType mapSqlType = (MapSqlType) relDataType;

            gen.writeFieldName(FIELD_NAME_KEY);
            gen.writeStartObject();
            serialize(mapSqlType.getKeyType(), gen);
            gen.writeEndObject();

            gen.writeFieldName(FIELD_NAME_VALUE);
            gen.writeStartObject();
            serialize(mapSqlType.getValueType(), gen);
            gen.writeEndObject();
        } else if (relDataType instanceof GenericRelDataType) {
            assert relDataType.getSqlTypeName() == SqlTypeName.ANY;
            serializeCommon(relDataType, gen);
            TypeInformationRawType<?> rawType = ((GenericRelDataType) relDataType).genericType();

            gen.writeFieldName(FIELD_NAME_RAW_TYPE);
            gen.writeStartObject();
            gen.writeBooleanField(FIELD_NAME_NULLABLE, rawType.isNullable());
            gen.writeStringField(
                    FIELD_NAME_TYPE_INFO,
                    EncodingUtils.encodeObjectToString(rawType.getTypeInformation()));
            gen.writeEndObject();
        } else if (relDataType instanceof RawRelDataType) {
            assert relDataType.getSqlTypeName() == SqlTypeName.OTHER;
            serializeCommon(relDataType, gen);
            RawRelDataType rawType = (RawRelDataType) relDataType;
            gen.writeStringField(FIELD_NAME_RAW_TYPE, rawType.getRawType().asSerializableString());
        } else {
            serializeCommon(relDataType, gen);
        }
    }

    private void serializeCommon(RelDataType relDataType, JsonGenerator gen) throws IOException {
        final SqlTypeName typeName = relDataType.getSqlTypeName();
        gen.writeStringField(FIELD_NAME_TYPE_NAME, typeName.name());
        gen.writeBooleanField(FIELD_NAME_NULLABLE, relDataType.isNullable());
        if (relDataType.getSqlTypeName().allowsPrec()) {
            gen.writeNumberField(FIELD_NAME_PRECISION, relDataType.getPrecision());
        }
        if (relDataType.getSqlTypeName().allowsScale()) {
            gen.writeNumberField(FIELD_NAME_SCALE, relDataType.getScale());
        }
    }
}
