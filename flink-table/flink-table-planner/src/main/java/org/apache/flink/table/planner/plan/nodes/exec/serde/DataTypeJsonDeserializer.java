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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.extraction.ExtractionUtils;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerializer.FIELD_NAME_CONVERSION_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerializer.FIELD_NAME_ELEMENT_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerializer.FIELD_NAME_FIELDS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerializer.FIELD_NAME_FIELD_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerializer.FIELD_NAME_KEY_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerializer.FIELD_NAME_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerializer.FIELD_NAME_VALUE_CLASS;

/**
 * JSON deserializer for {@link DataType}.
 *
 * @see DataTypeJsonSerializer for the reverse operation
 */
@Internal
public class DataTypeJsonDeserializer extends StdDeserializer<DataType> {

    public DataTypeJsonDeserializer() {
        super(DataType.class);
    }

    @Override
    public DataType deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        final JsonNode dataTypeNode = jsonParser.readValueAsTree();
        final SerdeContext serdeContext = SerdeContext.get(ctx);
        return deserialize(dataTypeNode, serdeContext);
    }

    public static DataType deserialize(JsonNode dataTypeNode, SerdeContext serdeContext) {
        if (dataTypeNode.isTextual()) {
            return deserializeWithInternalClass(dataTypeNode, serdeContext);
        } else {
            return deserializeWithExternalClass(dataTypeNode, serdeContext);
        }
    }

    private static DataType deserializeWithInternalClass(
            JsonNode logicalTypeNode, SerdeContext serdeContext) {
        final LogicalType logicalType =
                LogicalTypeJsonDeserializer.deserialize(logicalTypeNode, serdeContext);
        return DataTypes.of(logicalType).toInternal();
    }

    private static DataType deserializeWithExternalClass(
            JsonNode dataTypeNode, SerdeContext serdeContext) {
        final LogicalType logicalType =
                LogicalTypeJsonDeserializer.deserialize(
                        dataTypeNode.get(FIELD_NAME_TYPE), serdeContext);
        return deserializeClass(logicalType, dataTypeNode, serdeContext);
    }

    private static DataType deserializeClass(
            LogicalType logicalType, @Nullable JsonNode parentNode, SerdeContext serdeContext) {
        if (parentNode == null) {
            return DataTypes.of(logicalType).toInternal();
        }

        final DataType dataType;
        switch (logicalType.getTypeRoot()) {
            case ARRAY:
            case MULTISET:
                final DataType elementDataType =
                        deserializeClass(
                                logicalType.getChildren().get(0),
                                parentNode.get(FIELD_NAME_ELEMENT_CLASS),
                                serdeContext);
                dataType = new CollectionDataType(logicalType, elementDataType);
                break;

            case MAP:
                final MapType mapType = (MapType) logicalType;
                final DataType keyDataType =
                        deserializeClass(
                                mapType.getKeyType(),
                                parentNode.get(FIELD_NAME_KEY_CLASS),
                                serdeContext);
                final DataType valueDataType =
                        deserializeClass(
                                mapType.getValueType(),
                                parentNode.get(FIELD_NAME_VALUE_CLASS),
                                serdeContext);
                dataType = new KeyValueDataType(mapType, keyDataType, valueDataType);
                break;

            case ROW:
            case STRUCTURED_TYPE:
                final List<String> fieldNames = LogicalTypeChecks.getFieldNames(logicalType);
                final List<LogicalType> fieldTypes = LogicalTypeChecks.getFieldTypes(logicalType);

                final ArrayNode fieldNodes = (ArrayNode) parentNode.get(FIELD_NAME_FIELDS);
                final Map<String, JsonNode> fieldNodesByName = new HashMap<>();
                if (fieldNodes != null) {
                    fieldNodes.forEach(
                            fieldNode ->
                                    fieldNodesByName.put(
                                            fieldNode.get(FIELD_NAME_FIELD_NAME).asText(),
                                            fieldNode));
                }

                final List<DataType> fieldDataTypes =
                        IntStream.range(0, fieldNames.size())
                                .mapToObj(
                                        i -> {
                                            final String fieldName = fieldNames.get(i);
                                            final LogicalType fieldType = fieldTypes.get(i);
                                            return deserializeClass(
                                                    fieldType,
                                                    fieldNodesByName.get(fieldName),
                                                    serdeContext);
                                        })
                                .collect(Collectors.toList());

                dataType = new FieldsDataType(logicalType, fieldDataTypes);
                break;

            case DISTINCT_TYPE:
                final DistinctType distinctType = (DistinctType) logicalType;
                dataType = deserializeClass(distinctType.getSourceType(), parentNode, serdeContext);
                break;

            default:
                dataType = DataTypes.of(logicalType).toInternal();
        }

        final Class<?> conversionClass =
                loadClass(
                        parentNode.get(FIELD_NAME_CONVERSION_CLASS).asText(),
                        serdeContext,
                        String.format("conversion class of data type '%s'", dataType));
        return dataType.bridgedTo(conversionClass);
    }

    private static Class<?> loadClass(
            String className, SerdeContext serdeContext, String explanation) {
        try {
            return ExtractionUtils.classForName(className, true, serdeContext.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new TableException(
                    String.format("Could not load class '%s' for %s.", className, explanation), e);
        }
    }
}
