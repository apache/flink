/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.registry.confluent.utils;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.OneofDescriptor;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A converter from {@link com.google.protobuf.Descriptors.Descriptor} to {@link
 * org.apache.flink.table.types.logical.LogicalType}.
 *
 * <p>The mapping is represented by the following table:
 *
 * <pre>
 * +-----------------------------------------------------+-----------------------------------------------------+-------------------------+-------------------------------------------------------+
 * |                    Protobuf type                    |                    Message type                     | Connect type annotation |                      Flink type                       |
 * +-----------------------------------------------------+-----------------------------------------------------+-------------------------+-------------------------------------------------------+
 * | INT32|SINT32|SFIXED32                               | -                                                   |                         | INT                                                   |
 * | INT32|SINT32|SFIXED32                               | -                                                   | int8                    | TINYINT                                               |
 * | INT32|SINT32|SFIXED32                               | -                                                   | int16                   | SMALLINT                                              |
 * | UINT32|FIXED32|INT64|UINT64|SINT64|FIXED64|SFIXED64 | -                                                   | -                       | BIGINT                                                |
 * | FLOAT                                               | -                                                   | -                       | FLOAT                                                 |
 * | DOUBLE                                              | -                                                   | -                       | DOUBLE                                                |
 * | BOOL                                                | -                                                   | -                       | BOOLEAN                                               |
 * | ENUM                                                | -                                                   | -                       | VARCHAR                                               |
 * | STRING                                              | -                                                   | -                       | VARCHAR                                               |
 * | BYTES                                               | -                                                   | -                       | VARBINARY                                             |
 * | MESSAGE                                             | confluent.type.Decimal                              | -                       | DECIMAL                                               |
 * | MESSAGE                                             | google.type.Date                                    | -                       | DATE                                                  |
 * | MESSAGE                                             | google.type.TimeOfDay                               | -                       | TIME(3) // Flink does not support higher precision    |
 * | MESSAGE                                             | google.protobuf.Timestamp                           | -                       | TIMESTAMP_LTZ(9)                                      |
 * | MESSAGE                                             | google.protobuf.DoubleValue                         | -                       | DOUBLE                                                |
 * | MESSAGE                                             | google.protobuf.FloatValue                          | -                       | FLOAT                                                 |
 * | MESSAGE                                             | google.protobuf.Int32Value                          | -                       | INT                                                   |
 * | MESSAGE                                             | google.protobuf.Int64Value                          | -                       | BIGINT                                                |
 * | MESSAGE                                             | google.protobuf.UInt64Value                         | -                       | BIGINT                                                |
 * | MESSAGE                                             | google.protobuf.UInt32Value                         | -                       | BIGINT                                                |
 * | MESSAGE                                             | google.protobuf.BoolValue                           | -                       | BOOLEAN                                               |
 * | MESSAGE                                             | google.protobuf.StringValue                         | -                       | VARCHAR                                               |
 * | MESSAGE                                             | google.protobuf.BytesValue                          | -                       | VARBINARY                                             |
 * | MESSAGE                                             | repeated xx.xx.XXEntry([type1] key, [type2] value)  | -                       | MAP[type1, type2]                                     |
 * | MESSAGE                                             | -                                                   | -                       | ROW                                                   |
 * | oneOf                                               | -                                                   | -                       | ROW                                                   |
 * +-----------------------------------------------------+-----------------------------------------------------+-------------------------+-------------------------------------------------------+
 * </pre>
 *
 * <p>Expressing something as NULLABLE or NOT NULL is not straightforward in Protobuf.
 *
 * <ul>
 *   <li>all non MESSAGE types are NOT NULL (if not set explicitly default value is assigned)
 *   <li>non MESSAGE types marked with 'optional' can be checked if they were set. If not set we
 *       assume NULL
 *   <li>MESSAGE types are all NULLABLE, in other words all fields of a MESSAGE type are optional
 *       and there is no way to ensure on a format level they are NOT NULL
 *   <li>ARRAYS can not be NULL, not set repeated field is presented as an empty list, there is no
 *       way to differentiate an empty array from NULL
 * </ul>
 */
public class ProtoToFlinkSchemaConverter {

    /**
     * Mostly adapted the logic from <a
     * href="https://github.com/confluentinc/schema-registry/blob/610fbed58a3a8d778ec7a9de5b8d2d0c1465c6f9/protobuf-converter/src/main/java/io/confluent/connect/protobuf/ProtobufData.java">ProtobufData</a>
     * Should be kept in sync to handle all connect data types.
     */
    public static LogicalType toFlinkSchema(final Descriptor schema) {
        final CycleContext context = new CycleContext();
        // top-level row must not be NULLABLE in SQL, thus we change the nullability of the top row
        return toFlinkSchemaNested(schema, context).copy(false);
    }

    private static LogicalType toFlinkSchemaNested(
            final Descriptor schema, final CycleContext context) {
        List<OneofDescriptor> oneOfDescriptors = schema.getRealOneofs();
        final List<RowField> fields = new ArrayList<>();
        final List<RowField> oneOfFields = new ArrayList<>();
        for (OneofDescriptor oneOfDescriptor : oneOfDescriptors) {
            oneOfFields.add(
                    new RowField(
                            oneOfDescriptor.getName(), toFlinkSchema(oneOfDescriptor, context)));
        }
        List<FieldDescriptor> fieldDescriptors = schema.getFields();
        for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
            OneofDescriptor oneOfDescriptor = fieldDescriptor.getRealContainingOneof();
            if (oneOfDescriptor != null) {
                // Already added field as oneof
                continue;
            }
            fields.add(
                    new RowField(
                            fieldDescriptor.getName(), toFlinkSchema(fieldDescriptor, context)));
        }
        fields.addAll(oneOfFields);
        return new RowType(true, fields);
    }

    private static LogicalType toFlinkSchema(
            OneofDescriptor oneOfDescriptor, CycleContext context) {
        List<FieldDescriptor> fieldDescriptors = oneOfDescriptor.getFields();
        final List<RowField> fields =
                fieldDescriptors.stream()
                        .map(field -> new RowField(field.getName(), toFlinkSchema(field, context)))
                        .collect(Collectors.toList());
        return new RowType(true, fields);
    }

    private static LogicalType toFlinkSchema(
            final FieldDescriptor schema, final CycleContext context) {
        if (schema.isRepeated()) {
            return convertRepeated(schema, context);
        } else {
            return convertNonRepeated(schema, context);
        }
    }

    private static LogicalType convertNonRepeated(
            FieldDescriptor schema, CycleContext cycleContext) {
        final boolean isOptional = schema.hasOptionalKeyword();
        switch (schema.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
                {
                    if (schema.getOptions().hasExtension(MetaProto.fieldMeta)) {
                        Meta fieldMeta = schema.getOptions().getExtension(MetaProto.fieldMeta);
                        Map<String, String> params = fieldMeta.getParamsMap();
                        String connectType = params.get(CommonConstants.CONNECT_TYPE_PROP);
                        if (CommonConstants.CONNECT_TYPE_INT8.equals(connectType)) {
                            return new TinyIntType(isOptional);
                        } else if (CommonConstants.CONNECT_TYPE_INT16.equals(connectType)) {
                            return new SmallIntType(isOptional);
                        }
                    }
                    return new IntType(isOptional);
                }
            case UINT32:
            case FIXED32:
            case INT64:
            case UINT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                return new BigIntType(isOptional);
            case FLOAT:
                return new FloatType(isOptional);
            case DOUBLE:
                return new DoubleType(isOptional);
            case BOOL:
                return new BooleanType(isOptional);
            case ENUM:
            case STRING:
                return new VarCharType(isOptional, VarCharType.MAX_LENGTH);
            case BYTES:
                return new VarBinaryType(isOptional, VarBinaryType.MAX_LENGTH);
            case MESSAGE:
                {
                    String fullName = schema.getMessageType().getFullName();
                    switch (fullName) {
                        case CommonConstants.PROTOBUF_DECIMAL_TYPE:
                            int precision = DecimalType.DEFAULT_PRECISION;
                            int scale = DecimalType.DEFAULT_SCALE;
                            if (schema.getOptions().hasExtension(MetaProto.fieldMeta)) {
                                Meta fieldMeta =
                                        schema.getOptions().getExtension(MetaProto.fieldMeta);
                                Map<String, String> params = fieldMeta.getParamsMap();
                                String precisionStr =
                                        params.get(CommonConstants.PROTOBUF_PRECISION_PROP);
                                if (precisionStr != null) {
                                    try {
                                        precision = Integer.parseInt(precisionStr);
                                    } catch (NumberFormatException e) {
                                        // ignore
                                    }
                                }
                                String scaleStr = params.get(CommonConstants.PROTOBUF_SCALE_PROP);
                                if (scaleStr != null) {
                                    try {
                                        scale = Integer.parseInt(scaleStr);
                                    } catch (NumberFormatException e) {
                                        // ignore
                                    }
                                }
                            }
                            return new DecimalType(true, precision, scale);
                        case CommonConstants.PROTOBUF_DATE_TYPE:
                            return new DateType(true);
                        case CommonConstants.PROTOBUF_TIME_TYPE:
                            return new TimeType(true, 3);
                        case CommonConstants.PROTOBUF_TIMESTAMP_TYPE:
                            return new LocalZonedTimestampType(true, 9);
                        default:
                            if (cycleContext.seenMessage.contains(fullName)) {
                                throw new IllegalArgumentException(
                                        "Cyclic schemas are not supported.");
                            }
                            cycleContext.seenMessage.add(fullName);
                            final LogicalType recordSchema =
                                    toUnwrappedOrRecordSchema(schema, cycleContext);
                            cycleContext.seenMessage.remove(fullName);
                            return recordSchema;
                    }
                }
            default:
                throw new IllegalArgumentException("Unknown schema type: " + schema.getType());
        }
    }

    private static LogicalType convertRepeated(FieldDescriptor schema, CycleContext context) {
        if (isMapDescriptor(schema)) {
            return toMapSchema(schema.getMessageType(), context);
        } else {
            // repeated type is always NOT NULL
            return new ArrayType(false, convertNonRepeated(schema, context));
        }
    }

    private static LogicalType toUnwrappedOrRecordSchema(
            FieldDescriptor descriptor, CycleContext context) {
        return toUnwrappedSchema(descriptor.getMessageType())
                .orElseGet(() -> toFlinkSchemaNested(descriptor.getMessageType(), context));
    }

    private static Optional<LogicalType> toUnwrappedSchema(Descriptor descriptor) {
        String fullName = descriptor.getFullName();
        switch (fullName) {
            case CommonConstants.PROTOBUF_DOUBLE_WRAPPER_TYPE:
                return Optional.of(new DoubleType(true));
            case CommonConstants.PROTOBUF_FLOAT_WRAPPER_TYPE:
                return Optional.of(new FloatType(true));
            case CommonConstants.PROTOBUF_INT64_WRAPPER_TYPE:
            case CommonConstants.PROTOBUF_UINT64_WRAPPER_TYPE:
            case CommonConstants.PROTOBUF_UINT32_WRAPPER_TYPE:
                return Optional.of(new BigIntType(true));
            case CommonConstants.PROTOBUF_INT32_WRAPPER_TYPE:
                return Optional.of(new IntType(true));
            case CommonConstants.PROTOBUF_BOOL_WRAPPER_TYPE:
                return Optional.of(new BooleanType(true));
            case CommonConstants.PROTOBUF_STRING_WRAPPER_TYPE:
                return Optional.of(new VarCharType(true, VarCharType.MAX_LENGTH));
            case CommonConstants.PROTOBUF_BYTES_WRAPPER_TYPE:
                return Optional.of(new VarBinaryType(true, VarBinaryType.MAX_LENGTH));
            default:
                return Optional.empty();
        }
    }

    private static LogicalType toMapSchema(final Descriptor descriptor, CycleContext context) {
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
        // repeated type is always NOT NULL
        return new MapType(
                false,
                toFlinkSchema(fieldDescriptors.get(0), context),
                toFlinkSchema(fieldDescriptors.get(1), context));
    }

    private static boolean isMapDescriptor(FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getType() != Type.MESSAGE) {
            return false;
        }
        Descriptor descriptor = fieldDescriptor.getMessageType();
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
        return descriptor.getName().endsWith(CommonConstants.MAP_ENTRY_SUFFIX)
                && fieldDescriptors.size() == 2
                && fieldDescriptors.get(0).getName().equals(CommonConstants.KEY_FIELD)
                && fieldDescriptors.get(1).getName().equals(CommonConstants.VALUE_FIELD)
                && !fieldDescriptors.get(0).isRepeated()
                && !fieldDescriptors.get(1).isRepeated();
    }

    private static final class CycleContext {
        private final Set<String> seenMessage = new HashSet<>();
    }
}
