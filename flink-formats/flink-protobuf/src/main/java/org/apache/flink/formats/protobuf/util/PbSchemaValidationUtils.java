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

package org.apache.flink.formats.protobuf.util;

import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.EnumMap;
import java.util.EnumSet;

/** Validation class to verify protobuf definition and flink schema. */
public class PbSchemaValidationUtils {

    private static final EnumMap<JavaType, EnumSet<LogicalTypeRoot>> TYPE_MATCH_MAP =
            new EnumMap<>(JavaType.class);

    static {
        TYPE_MATCH_MAP.put(JavaType.BOOLEAN, EnumSet.of(LogicalTypeRoot.BOOLEAN));
        TYPE_MATCH_MAP.put(
                JavaType.BYTE_STRING,
                EnumSet.of(LogicalTypeRoot.BINARY, LogicalTypeRoot.VARBINARY));
        TYPE_MATCH_MAP.put(JavaType.DOUBLE, EnumSet.of(LogicalTypeRoot.DOUBLE));
        TYPE_MATCH_MAP.put(JavaType.FLOAT, EnumSet.of(LogicalTypeRoot.FLOAT));
        TYPE_MATCH_MAP.put(
                JavaType.ENUM,
                EnumSet.of(
                        LogicalTypeRoot.VARCHAR,
                        LogicalTypeRoot.CHAR,
                        LogicalTypeRoot.TINYINT,
                        LogicalTypeRoot.SMALLINT,
                        LogicalTypeRoot.INTEGER,
                        LogicalTypeRoot.BIGINT));
        TYPE_MATCH_MAP.put(
                JavaType.STRING, EnumSet.of(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.CHAR));
        TYPE_MATCH_MAP.put(JavaType.INT, EnumSet.of(LogicalTypeRoot.INTEGER));
        TYPE_MATCH_MAP.put(JavaType.LONG, EnumSet.of(LogicalTypeRoot.BIGINT));
    }

    public static void validate(Descriptors.Descriptor descriptor, RowType rowType) {
        validateTypeMatch(descriptor, rowType);
    }

    /**
     * Validate type match of row type.
     *
     * @param descriptor the {@link Descriptors.Descriptor} of the protobuf object.
     * @param rowType the corresponding {@link RowType} to the {@link Descriptors.Descriptor}
     */
    private static void validateTypeMatch(Descriptors.Descriptor descriptor, RowType rowType) {
        rowType.getFields()
                .forEach(
                        rowField -> {
                            FieldDescriptor fieldDescriptor =
                                    descriptor.findFieldByName(rowField.getName());
                            if (null != fieldDescriptor) {
                                validateTypeMatch(fieldDescriptor, rowField.getType());
                            } else {
                                throw new ValidationException(
                                        "Column "
                                                + rowField.getName()
                                                + " does not exists in definition of proto class.");
                            }
                        });
    }

    /**
     * Validate type match of general type.
     *
     * @param fd the {@link Descriptors.Descriptor} of the protobuf object.
     * @param logicalType the corresponding {@link LogicalType} to the {@link FieldDescriptor}
     */
    private static void validateTypeMatch(FieldDescriptor fd, LogicalType logicalType) {
        if (!fd.isRepeated()) {
            if (fd.getJavaType() != JavaType.MESSAGE) {
                // simple type
                validateSimpleType(fd, logicalType.getTypeRoot());
            } else {
                // message type
                if (!(logicalType instanceof RowType)) {
                    throw new ValidationException(
                            "Unexpected LogicalType: " + logicalType + ". It should be RowType");
                }
                validateTypeMatch(fd.getMessageType(), (RowType) logicalType);
            }
        } else {
            if (fd.isMapField()) {
                // map type
                if (!(logicalType instanceof MapType)) {
                    throw new ValidationException(
                            "Unexpected LogicalType: " + logicalType + ". It should be MapType");
                }
                MapType mapType = (MapType) logicalType;
                validateSimpleType(
                        fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME),
                        mapType.getKeyType().getTypeRoot());
                validateTypeMatch(
                        fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME),
                        mapType.getValueType());
            } else {
                // array type
                if (!(logicalType instanceof ArrayType)) {
                    throw new ValidationException(
                            "Unexpected LogicalType: " + logicalType + ". It should be ArrayType");
                }
                ArrayType arrayType = (ArrayType) logicalType;
                if (fd.getJavaType() == JavaType.MESSAGE) {
                    // array message type
                    LogicalType elementType = arrayType.getElementType();
                    if (!(elementType instanceof RowType)) {
                        throw new ValidationException(
                                "Unexpected logicalType: "
                                        + elementType
                                        + ". It should be RowType");
                    }
                    validateTypeMatch(fd.getMessageType(), (RowType) elementType);
                } else {
                    // array simple type
                    validateSimpleType(fd, arrayType.getElementType().getTypeRoot());
                }
            }
        }
    }

    /**
     * Only validate type match for simple type like int, long, string, boolean.
     *
     * @param fd {@link FieldDescriptor} in proto descriptor
     * @param logicalTypeRoot {@link LogicalTypeRoot} of row element
     */
    private static void validateSimpleType(FieldDescriptor fd, LogicalTypeRoot logicalTypeRoot) {
        if (!TYPE_MATCH_MAP.containsKey(fd.getJavaType())) {
            throw new ValidationException("Unsupported protobuf java type: " + fd.getJavaType());
        }
        if (TYPE_MATCH_MAP.get(fd.getJavaType()).stream().noneMatch(x -> x == logicalTypeRoot)) {
            throw new ValidationException(
                    "Protobuf field type does not match column type, "
                            + fd.getJavaType()
                            + "(protobuf) is not compatible of "
                            + logicalTypeRoot);
        }
    }
}
