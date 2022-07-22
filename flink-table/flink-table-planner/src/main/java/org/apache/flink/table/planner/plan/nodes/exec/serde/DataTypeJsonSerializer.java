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
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.utils.DataTypeUtils.isInternal;

/**
 * JSON serializer for {@link DataType}.
 *
 * @see DataTypeJsonDeserializer for the reverse operation
 */
@Internal
final class DataTypeJsonSerializer extends StdSerializer<DataType> {
    private static final long serialVersionUID = 1L;

    /*
    Example generated JSON for a data type with custom conversion classes:

        DataTypes.ROW(
            DataTypes.STRING().toInternal(),
            DataTypes.TIMESTAMP_LTZ().bridgedTo(Long.class),
            DataTypes.STRING())

        {
          "logicalType": "ROW<`f0` VARCHAR(2147483647), `f1` TIMESTAMP(6) WITH LOCAL TIME ZONE, `f2` VARCHAR(2147483647)>",
          "fields": [
            {
              "name": "f0",
              "conversionClass": "org.apache.flink.table.data.StringData"
            },
            {
              "name": "f1",
              "conversionClass": "java.lang.Long"
            }
          ]
        }

     Example generated JSON for a data type with only default conversion classes:

        DataTypes.ROW(DataTypes.STRING(), DataTypes.TIMESTAMP_LTZ())

        "ROW<`f0` VARCHAR(2147483647), `f1` TIMESTAMP(6) WITH LOCAL TIME ZONE>"

     Example generated JSON for a data type with only internal conversion classes:

        DataTypes.ROW(DataTypes.STRING(), DataTypes.TIMESTAMP_LTZ()).toInternal()

        {
          "logicalType": "ROW<`f0` VARCHAR(2147483647), `f1` TIMESTAMP(6) WITH LOCAL TIME ZONE>",
          "conversionClass": "org.apache.flink.table.data.RowData"
        }
     */

    // Common fields
    static final String FIELD_NAME_TYPE = "logicalType";
    static final String FIELD_NAME_CONVERSION_CLASS = "conversionClass";

    // ARRAY, MULTISET
    static final String FIELD_NAME_ELEMENT_CLASS = "elementClass";

    // MAP
    static final String FIELD_NAME_KEY_CLASS = "keyClass";
    static final String FIELD_NAME_VALUE_CLASS = "valueClass";

    // ROW, STRUCTURED_TYPE, DISTINCT_TYPE
    static final String FIELD_NAME_FIELDS = "fields";
    static final String FIELD_NAME_FIELD_NAME = "name";
    static final String FIELD_NAME_FIELD_CLASS = "fieldClass";

    DataTypeJsonSerializer() {
        super(DataType.class);
    }

    @Override
    public void serialize(
            DataType dataType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        if (isDefaultClassNested(dataType)) {
            serializerProvider.defaultSerializeValue(dataType.getLogicalType(), jsonGenerator);
        } else {
            jsonGenerator.writeStartObject();
            serializerProvider.defaultSerializeField(
                    FIELD_NAME_TYPE, dataType.getLogicalType(), jsonGenerator);
            serializeClass(dataType, jsonGenerator);
            jsonGenerator.writeEndObject();
        }
    }

    private static void serializeClass(DataType dataType, JsonGenerator jsonGenerator)
            throws IOException {
        // skip the conversion class if only nested types contain custom conversion classes
        if (!isDefaultClass(dataType)) {
            jsonGenerator.writeStringField(
                    FIELD_NAME_CONVERSION_CLASS, dataType.getConversionClass().getName());
        }
        // internal classes only contain nested internal classes
        if (isInternal(dataType, false)) {
            return;
        }

        switch (dataType.getLogicalType().getTypeRoot()) {
            case ARRAY:
            case MULTISET:
                final CollectionDataType collectionDataType = (CollectionDataType) dataType;
                serializeFieldIfNotDefaultClass(
                        collectionDataType.getElementDataType(),
                        FIELD_NAME_ELEMENT_CLASS,
                        jsonGenerator);
                break;
            case MAP:
                final KeyValueDataType keyValueDataType = (KeyValueDataType) dataType;
                serializeFieldIfNotDefaultClass(
                        keyValueDataType.getKeyDataType(), FIELD_NAME_KEY_CLASS, jsonGenerator);
                serializeFieldIfNotDefaultClass(
                        keyValueDataType.getValueDataType(), FIELD_NAME_VALUE_CLASS, jsonGenerator);
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final List<Field> nonDefaultFields =
                        DataType.getFields(dataType).stream()
                                .filter(field -> !isDefaultClassNested(field.getDataType()))
                                .collect(Collectors.toList());
                if (nonDefaultFields.isEmpty()) {
                    break;
                }
                jsonGenerator.writeFieldName(FIELD_NAME_FIELDS);
                jsonGenerator.writeStartArray();
                for (Field nonDefaultField : nonDefaultFields) {
                    jsonGenerator.writeStartObject();
                    jsonGenerator.writeStringField(
                            FIELD_NAME_FIELD_NAME, nonDefaultField.getName());
                    serializeClass(nonDefaultField.getDataType(), jsonGenerator);
                    jsonGenerator.writeEndObject();
                }
                jsonGenerator.writeEndArray();
                break;
            case DISTINCT_TYPE:
                final DataType sourceDataType = dataType.getChildren().get(0);
                if (!isDefaultClassNested(sourceDataType)) {
                    serializeClass(sourceDataType, jsonGenerator);
                }
                break;
            default:
                // for data types without children
        }
    }

    private static void serializeFieldIfNotDefaultClass(
            DataType dataType, String fieldName, JsonGenerator jsonGenerator) throws IOException {
        if (!isDefaultClassNested(dataType)) {
            jsonGenerator.writeFieldName(fieldName);
            jsonGenerator.writeStartObject();
            serializeClass(dataType, jsonGenerator);
            jsonGenerator.writeEndObject();
        }
    }

    private static boolean isDefaultClassNested(DataType dataType) {
        return isDefaultClass(dataType)
                && dataType.getChildren().stream()
                        .allMatch(DataTypeJsonSerializer::isDefaultClassNested);
    }

    private static boolean isDefaultClass(DataType dataType) {
        return Objects.equals(
                dataType.getConversionClass(), dataType.getLogicalType().getDefaultConversion());
    }
}
