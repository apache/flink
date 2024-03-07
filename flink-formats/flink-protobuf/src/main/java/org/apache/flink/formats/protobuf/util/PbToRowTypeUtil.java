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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Generate Row type information according to pb descriptors. */
public class PbToRowTypeUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PbToRowTypeUtil.class);

    public static RowType generateRowType(Descriptors.Descriptor root) {
        return generateRowType(root, false, 0);
    }

    public static RowType generateRowType(Descriptors.Descriptor root, boolean enumAsInt) {
        return generateRowType(root, enumAsInt, 0);
    }

    public static RowType generateRowType(Descriptors.Descriptor root, int recursiveFieldMaxDepth) {
        return generateRowType(root, false, recursiveFieldMaxDepth);
    }

    /**
     * Generates RowType for a specified generated protobuf class.
     *
     * @param recursiveFieldMaxDepth controls the depth for recursive schema. Default value is 0.
     *     The next occurrence of recursive field after `recursiveFieldMaxDepth` level will be
     *     omitted.
     */
    public static RowType generateRowType(
            Descriptors.Descriptor root, boolean enumAsInt, int recursiveFieldMaxDepth) {
        Map<String, Integer> existingRecordNames = new HashMap<>();
        existingRecordNames.put(root.getFullName(), 1);
        return generateRowTypeHelper(root, enumAsInt, recursiveFieldMaxDepth, existingRecordNames);
    }

    private static RowType generateRowTypeHelper(
            Descriptors.Descriptor root,
            boolean enumAsInt,
            int recursiveFieldMaxDepth,
            Map<String, Integer> existingRecordNames) {
        int size = root.getFields().size();
        List<LogicalType> types = new ArrayList<>();
        List<String> rowFieldNames = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            FieldDescriptor field = root.getFields().get(i);
            String fieldName = field.getName();
            JavaType fieldType = field.getJavaType();

            Map<String, Integer> updatedRecordNames = new HashMap<>(existingRecordNames);
            if (fieldType.equals(JavaType.MESSAGE)) {

                String fieldFullName = field.getMessageType().getFullName();

                if (existingRecordNames.getOrDefault(fieldFullName, 0) > recursiveFieldMaxDepth) {
                    LOG.info(
                            "Skipping a recursive field {} of type {} on level {}",
                            fieldName,
                            fieldFullName,
                            recursiveFieldMaxDepth + 1);
                    continue;
                }

                int count =
                        updatedRecordNames.containsKey(fieldFullName)
                                ? updatedRecordNames.get(fieldFullName)
                                : 0;
                updatedRecordNames.put(fieldFullName, count + 1);
            }
            rowFieldNames.add(fieldName);
            types.add(
                    generateFieldTypeInformation(
                            field, enumAsInt, recursiveFieldMaxDepth, updatedRecordNames));
        }
        return RowType.of(
                types.toArray(new LogicalType[types.size()]),
                rowFieldNames.toArray(new String[rowFieldNames.size()]));
    }

    private static LogicalType generateFieldTypeInformation(
            FieldDescriptor field,
            boolean enumAsInt,
            int recursiveFieldMaxDepth,
            Map<String, Integer> existingRecordNames) {
        JavaType fieldType = field.getJavaType();
        LogicalType type;
        if (fieldType.equals(JavaType.MESSAGE)) {
            if (field.isMapField()) {
                MapType mapType =
                        new MapType(
                                generateFieldTypeInformation(
                                        field.getMessageType()
                                                .findFieldByName(PbConstant.PB_MAP_KEY_NAME),
                                        enumAsInt,
                                        recursiveFieldMaxDepth,
                                        existingRecordNames),
                                generateFieldTypeInformation(
                                        field.getMessageType()
                                                .findFieldByName(PbConstant.PB_MAP_VALUE_NAME),
                                        enumAsInt,
                                        recursiveFieldMaxDepth,
                                        existingRecordNames));
                return mapType;
            } else if (field.isRepeated()) {
                return new ArrayType(
                        generateRowTypeHelper(
                                field.getMessageType(),
                                enumAsInt,
                                recursiveFieldMaxDepth,
                                existingRecordNames));
            } else {
                return generateRowTypeHelper(
                        field.getMessageType(),
                        enumAsInt,
                        recursiveFieldMaxDepth,
                        existingRecordNames);
            }
        } else {
            if (fieldType.equals(JavaType.STRING)) {
                type = new VarCharType(Integer.MAX_VALUE);
            } else if (fieldType.equals(JavaType.LONG)) {
                type = new BigIntType();
            } else if (fieldType.equals(JavaType.BOOLEAN)) {
                type = new BooleanType();
            } else if (fieldType.equals(JavaType.INT)) {
                type = new IntType();
            } else if (fieldType.equals(JavaType.DOUBLE)) {
                type = new DoubleType();
            } else if (fieldType.equals(JavaType.FLOAT)) {
                type = new FloatType();
            } else if (fieldType.equals(JavaType.ENUM)) {
                if (enumAsInt) {
                    type = new IntType();
                } else {
                    type = new VarCharType(Integer.MAX_VALUE);
                }
            } else if (fieldType.equals(JavaType.BYTE_STRING)) {
                type = new VarBinaryType(Integer.MAX_VALUE);
            } else {
                throw new ValidationException("unsupported field type: " + fieldType);
            }
            if (field.isRepeated()) {
                return new ArrayType(type);
            }
            return type;
        }
    }
}
