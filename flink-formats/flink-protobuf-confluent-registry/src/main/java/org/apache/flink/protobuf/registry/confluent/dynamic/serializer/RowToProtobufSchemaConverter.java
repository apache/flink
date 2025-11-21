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

package org.apache.flink.protobuf.registry.confluent.dynamic.serializer;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.commons.text.CaseUtils;

import java.util.List;

/**
 * Converts a Flink {@link RowType} to a {@link ProtobufSchema}. The generated schema can be used to
 * serialize Flink RowData to Protobuf.
 */
public class RowToProtobufSchemaConverter {
    private static final String NESTED_MESSAGE_SUFFIX = "Message";
    private static final String MAP_ENTRY_SUFFIX = "Entry";

    private final String packageName;
    private final String className;
    private final RowType rowType;

    public RowToProtobufSchemaConverter(String packageName, String className, RowType rowType) {
        this.packageName = packageName;
        this.className = className;
        this.rowType = rowType;
    }

    public ProtobufSchema convert() throws Descriptors.DescriptorValidationException {

        // Create a FileDescriptorProto builder
        DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder =
                DescriptorProtos.FileDescriptorProto.newBuilder();
        fileDescriptorProtoBuilder.setName(className);
        fileDescriptorProtoBuilder.setPackage(packageName);
        fileDescriptorProtoBuilder.setSyntax("proto3");
        fileDescriptorProtoBuilder.addDependency("google/protobuf/timestamp.proto");

        // Create a DescriptorProto builder for the message type
        DescriptorProtos.DescriptorProto.Builder descriptorProtoBuilder =
                DescriptorProtos.DescriptorProto.newBuilder();
        descriptorProtoBuilder.setName(className);

        // Convert each field in RowType to a FieldDescriptorProto
        recurseRowType(rowType, descriptorProtoBuilder);

        // Add the message type to the file descriptor
        fileDescriptorProtoBuilder.addMessageType(descriptorProtoBuilder);

        // Build the FileDescriptor
        DescriptorProtos.FileDescriptorProto fileDescriptorProto =
                fileDescriptorProtoBuilder.build();
        Descriptors.FileDescriptor[] dependencies =
                new Descriptors.FileDescriptor[] {Timestamp.getDescriptor().getFile()};
        Descriptors.FileDescriptor fileDescriptor =
                Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);

        return new ProtobufSchema(fileDescriptor);
    }

    /** Recursively traverse a RowType and convert its fields to proto message fields. */
    private static void recurseRowType(
            RowType rowType, DescriptorProtos.DescriptorProto.Builder currentBuilder) {

        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {

            RowType.RowField field = fields.get(i);
            DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorProtoBuilder =
                    DescriptorProtos.FieldDescriptorProto.newBuilder();
            fieldDescriptorProtoBuilder.setName(field.getName());
            fieldDescriptorProtoBuilder.setNumber(i + 1);

            if (field.getType() instanceof RowType) {

                String nestedTypeName =
                        addFlinkRowTypeToDescriptor(
                                field, (RowType) field.getType(), currentBuilder);
                fieldDescriptorProtoBuilder.setType(Type.TYPE_MESSAGE);
                fieldDescriptorProtoBuilder.setTypeName(nestedTypeName);

            } else if (field.getType() instanceof ArrayType) {

                if (((ArrayType) field.getType()).getElementType() instanceof RowType) {
                    String nestedTypeName =
                            addFlinkRowTypeToDescriptor(
                                    field,
                                    (RowType) ((ArrayType) field.getType()).getElementType(),
                                    currentBuilder);
                    fieldDescriptorProtoBuilder.setType(Type.TYPE_MESSAGE);
                    fieldDescriptorProtoBuilder.setTypeName(nestedTypeName);
                } else {
                    addPrimitiveFlinkFieldToDescriptor(
                            fieldDescriptorProtoBuilder,
                            ((ArrayType) field.getType()).getElementType());
                }

                fieldDescriptorProtoBuilder.setLabel(
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);

            } else if (field.getType() instanceof MapType) {

                String nestedTypeName =
                        addFlinkMapTypeToDescriptor(
                                field, (MapType) field.getType(), currentBuilder);
                fieldDescriptorProtoBuilder.setType(Type.TYPE_MESSAGE);
                fieldDescriptorProtoBuilder.setTypeName(nestedTypeName);
                fieldDescriptorProtoBuilder.setLabel(
                        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);

            } else {
                addPrimitiveFlinkFieldToDescriptor(fieldDescriptorProtoBuilder, field.getType());
            }
            currentBuilder.addField(fieldDescriptorProtoBuilder.build());
        }
    }

    private static void addPrimitiveFlinkFieldToDescriptor(
            DescriptorProtos.FieldDescriptorProto.Builder input, LogicalType rowFieldType) {
        switch (rowFieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                input.setType(Type.TYPE_STRING);
                return;
            case BOOLEAN:
                input.setType(Type.TYPE_BOOL);
                return;
            case BINARY:
            case VARBINARY:
                input.setType(Type.TYPE_BYTES);
                return;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                input.setType(Type.TYPE_INT32);
                return;
            case BIGINT:
                input.setType(Type.TYPE_INT64);
                return;
            case FLOAT:
                input.setType(Type.TYPE_FLOAT);
                return;
            case DOUBLE:
                input.setType(Type.TYPE_DOUBLE);
                return;
            default:
                throw new IllegalArgumentException("Unsupported logical type: " + rowFieldType);
        }
    }

    private static String addFlinkRowTypeToDescriptor(
            RowType.RowField field,
            RowType rowType,
            DescriptorProtos.DescriptorProto.Builder currentBuilder) {

        if (isTimestampType(rowType)) {
            return "google.protobuf.Timestamp";
        }

        String nestedTypeName = field.getName() + NESTED_MESSAGE_SUFFIX;

        DescriptorProtos.DescriptorProto.Builder newBuilder =
                DescriptorProtos.DescriptorProto.newBuilder();
        newBuilder.setName(nestedTypeName);
        recurseRowType(rowType, newBuilder);
        currentBuilder.addNestedType(newBuilder);
        return nestedTypeName;
    }

    private static String addFlinkMapTypeToDescriptor(
            RowType.RowField field,
            MapType mapType,
            DescriptorProtos.DescriptorProto.Builder currentBuilder) {

        DescriptorProtos.FieldDescriptorProto.Builder keyFieldDescriptorProtoBuilder =
                DescriptorProtos.FieldDescriptorProto.newBuilder();
        keyFieldDescriptorProtoBuilder.setName("key");
        keyFieldDescriptorProtoBuilder.setNumber(1);

        if (mapType.getKeyType() instanceof RowType) {
            DescriptorProtos.DescriptorProto.Builder keyMessageProtoBuilder =
                    DescriptorProtos.DescriptorProto.newBuilder();
            recurseRowType((RowType) mapType.getKeyType(), currentBuilder);
            String keyTypeName = field.getName() + "Key" + NESTED_MESSAGE_SUFFIX;
            keyMessageProtoBuilder.setName(keyTypeName);
            currentBuilder.addNestedType(keyMessageProtoBuilder);

            keyFieldDescriptorProtoBuilder.setType(Type.TYPE_MESSAGE);
            keyFieldDescriptorProtoBuilder.setTypeName(keyTypeName);
        } else {
            addPrimitiveFlinkFieldToDescriptor(
                    keyFieldDescriptorProtoBuilder, mapType.getKeyType());
        }

        DescriptorProtos.FieldDescriptorProto.Builder valueFieldDescriptorProtoBuilder =
                DescriptorProtos.FieldDescriptorProto.newBuilder();
        valueFieldDescriptorProtoBuilder.setName("value");
        valueFieldDescriptorProtoBuilder.setNumber(2);

        if (mapType.getValueType() instanceof RowType) {
            DescriptorProtos.DescriptorProto.Builder valueMessageProtoBuilder =
                    DescriptorProtos.DescriptorProto.newBuilder();
            recurseRowType((RowType) mapType.getValueType(), valueMessageProtoBuilder);
            String valueTypeName = field.getName() + "Value" + NESTED_MESSAGE_SUFFIX;
            valueMessageProtoBuilder.setName(valueTypeName);
            currentBuilder.addNestedType(valueMessageProtoBuilder);

            valueFieldDescriptorProtoBuilder.setType(Type.TYPE_MESSAGE);
            valueFieldDescriptorProtoBuilder.setTypeName(valueTypeName);
        } else {
            addPrimitiveFlinkFieldToDescriptor(
                    valueFieldDescriptorProtoBuilder, mapType.getValueType());
        }

        DescriptorProtos.DescriptorProto.Builder newBuilder =
                DescriptorProtos.DescriptorProto.newBuilder();

        // This is a very obscure part of the protobuf format. If the type isn't called fieldName +
        // "Entry",
        // the proto compiler won't be able to parse the definition.
        String camelCasedFieldName = CaseUtils.toCamelCase(field.getName(), true, '_');
        String nestedTypeName = camelCasedFieldName + MAP_ENTRY_SUFFIX;
        newBuilder.setName(nestedTypeName);

        newBuilder.addField(keyFieldDescriptorProtoBuilder.build());
        newBuilder.addField(valueFieldDescriptorProtoBuilder.build());
        newBuilder.setOptions(
                DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true).build());

        currentBuilder.addNestedType(newBuilder);
        return nestedTypeName;
    }

    // Auto-detect rows that should be represented as proto timestamps
    private static boolean isTimestampType(RowType rowType) {
        return rowType.getFields().size() == 2
                && rowType.getFields().get(0).getName().equals("seconds")
                && rowType.getFields().get(1).getName().equals("nanos")
                && rowType.getFields().get(0).getType().getTypeRoot().equals(LogicalTypeRoot.BIGINT)
                && rowType.getFields()
                        .get(1)
                        .getType()
                        .getTypeRoot()
                        .equals(LogicalTypeRoot.INTEGER);
    }
}
