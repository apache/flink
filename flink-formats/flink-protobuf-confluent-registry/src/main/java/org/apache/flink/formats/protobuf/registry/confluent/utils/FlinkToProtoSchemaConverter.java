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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import org.apache.flink.shaded.guava31.com.google.common.base.CaseFormat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto.Builder;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import io.confluent.protobuf.type.Decimal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * A converter from {@link org.apache.flink.table.types.logical.LogicalType} to {@link
 * com.google.protobuf.Descriptors.Descriptor}.
 *
 * <p>The mapping is represented by the following table:
 *
 * <pre>
 * +------------------------+------------------+---------------------------+-------------------------------------------+
 * |       Flink type       |  Protobuf type   |       Message type        |                  Comment                  |
 * +------------------------+------------------+---------------------------+-------------------------------------------+
 * | BOOLEAN                | BOOL             |                           |                                           |
 * | TINYINT                | INT32            | -                         | MetaProto extension: connect.type = int8  |
 * | SMALLINT               | INT32            | -                         | MetaProto extension: connect.type = int16 |
 * | INT                    | INT32            |                           |                                           |
 * | BIGINT                 | INT64            |                           |                                           |
 * | FLOAT                  | FLOAT            |                           |                                           |
 * | DOUBLE                 | DOUBLE           |                           |                                           |
 * | CHAR                   | STRING           |                           |                                           |
 * | VARCHAR                | STRING           |                           |                                           |
 * | BINARY                 | BYTES            |                           |                                           |
 * | VARBINARY              | BYTES            |                           |                                           |
 * | TIMESTAMP_LTZ          | MESSAGE          | google.protobuf.Timestamp |                                           |
 * | DATE                   | MESSAGE          | google.type.Date          |                                           |
 * | TIME_WITHOUT_TIME_ZONE | MESSAGE          | google.type.TimeOfDay     |                                           |
 * | DECIMAL                | MESSAGE          | confluent.type.Decimal    |                                           |
 * | MAP[K, V]              | repeated MESSAGE | XXEntry(K key, V value)   |                                           |
 * | ARRAY[T]               | repeated T       |                           |                                           |
 * | ROW                    | MESSAGE          | fieldName                 |                                           |
 * +------------------------+------------------+---------------------------+-------------------------------------------+
 * </pre>
 *
 * <p>When converting to a Protobuf schema we mark all NULLABLE fields as optional.
 */
public class FlinkToProtoSchemaConverter {

    /**
     * Converts a Flink's logical type into a Protobuf descriptor. Uses Kafka Connect logic to store
     * types that are not natively supported.
     */
    public static Descriptor fromFlinkSchema(
            RowType logicalType, String rowName, String packageName) {
        try {
            final Set<String> dependencies = new TreeSet<>();
            final DescriptorProto builder = fromRowType(logicalType, rowName, dependencies);
            final FileDescriptorProto fileDescriptorProto =
                    FileDescriptorProto.newBuilder()
                            .addMessageType(builder)
                            .setPackage(packageName)
                            .setSyntax("proto3")
                            .addAllDependency(dependencies)
                            .build();
            return FileDescriptor.buildFrom(
                            fileDescriptorProto,
                            Stream.of(
                                            Date.getDescriptor(),
                                            TimeOfDay.getDescriptor(),
                                            Timestamp.getDescriptor(),
                                            Decimal.getDescriptor())
                                    .map(Descriptor::getFile)
                                    .toArray(FileDescriptor[]::new))
                    .getFile()
                    .findMessageTypeByName(rowName);
        } catch (DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }

    public static ProtobufSchema fromFlinkRowType(
            RowType rowType, String rowName, String packageName) {
        return new ProtobufSchema(fromFlinkSchema(rowType, rowName, packageName));
    }

    private static DescriptorProto fromRowType(
            RowType logicalType, String rowName, Set<String> dependencies) {
        final Builder builder = DescriptorProto.newBuilder();

        builder.setName(rowName);
        final List<DescriptorProto> nestedRows = new ArrayList<>();
        final List<RowField> fields = logicalType.getFields();
        for (int i = 0; i < logicalType.getFieldCount(); i++) {
            final RowField field = fields.get(i);
            builder.addField(
                    fromRowField(
                            field.getType(),
                            field.getName(),
                            i + 1,
                            nestedRows,
                            dependencies,
                            false));
        }
        builder.addAllNestedType(nestedRows);
        return builder.build();
    }

    private static FieldDescriptorProto fromRowField(
            LogicalType logicalType,
            String fieldName,
            int fieldIndex,
            List<DescriptorProto> nestedRows,
            Set<String> dependencies,
            boolean isArray) {
        final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
        builder.setName(fieldName);
        builder.setNumber(fieldIndex);
        if (isArray) {
            builder.setLabel(Label.LABEL_REPEATED);
        } else if (!logicalType.isNullable()) {
            builder.setLabel(Label.LABEL_REQUIRED);
        } else {
            builder.setProto3Optional(logicalType.isNullable());
        }

        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                builder.setType(Type.TYPE_BOOL);
                return builder.build();
            case TINYINT:
                builder.setType(Type.TYPE_INT32);
                builder.setOptions(
                        FieldOptions.newBuilder()
                                .setExtension(
                                        MetaProto.fieldMeta,
                                        Meta.newBuilder()
                                                .putParams(
                                                        CommonConstants.CONNECT_TYPE_PROP,
                                                        CommonConstants.CONNECT_TYPE_INT8)
                                                .build())
                                .build());
                return builder.build();
            case SMALLINT:
                builder.setType(Type.TYPE_INT32);
                builder.setOptions(
                        FieldOptions.newBuilder()
                                .setExtension(
                                        MetaProto.fieldMeta,
                                        Meta.newBuilder()
                                                .putParams(
                                                        CommonConstants.CONNECT_TYPE_PROP,
                                                        CommonConstants.CONNECT_TYPE_INT16)
                                                .build())
                                .build());
                return builder.build();
            case INTEGER:
                builder.setType(Type.TYPE_INT32);
                return builder.build();
            case BIGINT:
                builder.setType(Type.TYPE_INT64);
                return builder.build();
            case FLOAT:
                builder.setType(Type.TYPE_FLOAT);
                return builder.build();
            case DOUBLE:
                builder.setType(Type.TYPE_DOUBLE);
                return builder.build();
            case CHAR:
            case VARCHAR:
                builder.setType(Type.TYPE_STRING);
                return builder.build();
            case BINARY:
            case VARBINARY:
                builder.setType(Type.TYPE_BYTES);
                return builder.build();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                builder.setType(Type.TYPE_MESSAGE);
                builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_TIMESTAMP_TYPE));
                dependencies.add(CommonConstants.PROTOBUF_TIMESTAMP_LOCATION);
                return builder.build();
            case DATE:
                builder.setType(Type.TYPE_MESSAGE);
                builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_DATE_TYPE));
                dependencies.add(CommonConstants.PROTOBUF_DATE_LOCATION);
                return builder.build();
            case TIME_WITHOUT_TIME_ZONE:
                builder.setType(Type.TYPE_MESSAGE);
                builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_TIME_TYPE));
                dependencies.add(CommonConstants.PROTOBUF_TIME_LOCATION);
                return builder.build();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                builder.setType(Type.TYPE_MESSAGE);
                builder.setOptions(
                        FieldOptions.newBuilder()
                                .setExtension(
                                        MetaProto.fieldMeta,
                                        Meta.newBuilder()
                                                .putParams(
                                                        CommonConstants.PROTOBUF_PRECISION_PROP,
                                                        String.valueOf(decimalType.getPrecision()))
                                                .putParams(
                                                        CommonConstants.PROTOBUF_SCALE_PROP,
                                                        String.valueOf(decimalType.getScale()))
                                                .build())
                                .build());
                builder.setTypeName(makeItTopLevelScoped(CommonConstants.PROTOBUF_DECIMAL_TYPE));
                dependencies.add(CommonConstants.PROTOBUF_DECIMAL_LOCATION);
                return builder.build();
            case ROW:
                {
                    // field name uniqueness should suffice for type naming. Each type is scoped to
                    // the enclosing Row. If a Row is nested within a nested Row, those two won't
                    // have collisions. Thus it is possible to have:
                    // message A {
                    //  b_Row b
                    //  message b_Row {
                    //    b_Row b
                    //    message b_Row {
                    //      int32 c;
                    //    }
                    //  }
                    // }
                    final String typeName = fieldName + "_Row";
                    final DescriptorProto nestedRowDescriptor =
                            fromRowType((RowType) logicalType, typeName, dependencies);
                    nestedRows.add(nestedRowDescriptor);
                    builder.setType(Type.TYPE_MESSAGE);
                    builder.setTypeName(typeName);
                    return builder.build();
                }
            case MAP:
                {
                    final MapType mapType = (MapType) logicalType;
                    return createMapLikeField(
                            fieldName,
                            fieldIndex,
                            nestedRows,
                            dependencies,
                            mapType.getKeyType(),
                            mapType.getValueType(),
                            mapType.isNullable());
                }
            case ARRAY:
                return fromRowField(
                        ((ArrayType) logicalType).getElementType(),
                        fieldName,
                        fieldIndex,
                        nestedRows,
                        dependencies,
                        true);
            case MULTISET:
                {
                    final MultisetType multisetType = (MultisetType) logicalType;
                    return createMapLikeField(
                            fieldName,
                            fieldIndex,
                            nestedRows,
                            dependencies,
                            multisetType.getElementType(),
                            new IntType(false),
                            multisetType.isNullable());
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case SYMBOL:
            case UNRESOLVED:
            case RAW:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported to derive Schema for type: " + logicalType);
        }
    }

    private static String makeItTopLevelScoped(String type) {
        // we scope types to the top level by prepending them with a dot. otherwise protobuf looks
        // for the types in the current scope. This makes it especially difficult if the current
        // scope has a common prefix with the given type e.g. io.confluent.generated.Row and
        // io.confluent.type.Decimal.
        return "." + type;
    }

    private static FieldDescriptorProto createMapLikeField(
            String fieldName,
            int fieldIndex,
            List<DescriptorProto> nestedRows,
            Set<String> dependencies,
            LogicalType keyType,
            LogicalType valueType,
            boolean isNullable) {
        final FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder();
        // Protobuf does not have a native support for a MAP type. It does represent a syntactic
        // sugar such as:
        //  message A { map<string, int32> map_field } is equivalent to:
        //  message A { repeated MapFieldEntry map_field message MapFieldEntry { string key, int32
        // value}}
        // we keep the naming strategy compatible here
        final String typeName =
                CaseFormat.LOWER_UNDERSCORE.to(
                        CaseFormat.UPPER_CAMEL, fieldName + "_" + CommonConstants.MAP_ENTRY_SUFFIX);
        final DescriptorProto mapDescriptor =
                fromRowType(
                        new RowType(
                                isNullable,
                                Arrays.asList(
                                        new RowField(CommonConstants.KEY_FIELD, keyType),
                                        new RowField(CommonConstants.VALUE_FIELD, valueType))),
                        typeName,
                        dependencies);
        nestedRows.add(mapDescriptor);
        builder.setType(Type.TYPE_MESSAGE);
        builder.setTypeName(typeName);
        builder.setNumber(fieldIndex);
        builder.setLabel(Label.LABEL_REPEATED);
        builder.setName(fieldName);
        builder.clearProto3Optional();
        return builder.build();
    }
}
