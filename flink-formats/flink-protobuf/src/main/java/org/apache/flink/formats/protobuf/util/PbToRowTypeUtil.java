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

import java.util.HashSet;
import java.util.Set;

/** Generate Row type information according to pb descriptors. */
public class PbToRowTypeUtil {
    public static RowType generateRowType(Descriptors.Descriptor root) {
        return generateRowType(root, false);
    }

    public static RowType generateRowType(Descriptors.Descriptor root, boolean enumAsInt) {
        // Track message types currently being resolved in the ancestor chain to detect
        // recursive references (e.g., A -> B -> A). Without this, recursive proto
        // definitions cause infinite recursion and StackOverflowError.
        Set<String> ancestors = new HashSet<>();
        return generateRowTypeInternal(root, enumAsInt, ancestors);
    }

    /**
     * @param ancestors message type full names currently being resolved in the call stack. Used to
     *     detect cycles: if a field's message type is already in this set, it's a recursive
     *     reference and gets emitted as BYTES instead of recursing infinitely.
     */
    private static RowType generateRowTypeInternal(
            Descriptors.Descriptor root, boolean enumAsInt, Set<String> ancestors) {
        int size = root.getFields().size();
        LogicalType[] types = new LogicalType[size];
        String[] rowFieldNames = new String[size];

        // Mark this type as "being resolved" before processing its fields
        String fullName = root.getFullName();
        ancestors.add(fullName);

        try {
            for (int i = 0; i < size; i++) {
                FieldDescriptor field = root.getFields().get(i);
                rowFieldNames[i] = field.getName();
                types[i] = generateFieldTypeInformation(field, enumAsInt, ancestors);
            }
        } finally {
            // Unmark when we're done - sibling branches shouldn't see this type as an ancestor
            ancestors.remove(fullName);
        }
        return RowType.of(types, rowFieldNames);
    }

    private static LogicalType generateFieldTypeInformation(
            FieldDescriptor field, boolean enumAsInt, Set<String> ancestors) {
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
                                        ancestors),
                                generateFieldTypeInformation(
                                        field.getMessageType()
                                                .findFieldByName(PbConstant.PB_MAP_VALUE_NAME),
                                        enumAsInt,
                                        ancestors));
                return mapType;
            }

            // Cycle detection: if this field's message type is already being resolved
            // in the ancestor chain, we have a recursive proto definition
            // (e.g., Node -> Child -> Node). Columnar schemas cannot represent
            // infinite recursion, so we emit the field as raw BYTES. The protobuf
            // binary is preserved and can be deserialized on demand if consumers
            // need the recursive data.
            String msgFullName = field.getMessageType().getFullName();
            if (ancestors.contains(msgFullName)) {
                LogicalType bytesType = new VarBinaryType(Integer.MAX_VALUE);
                if (field.isRepeated()) {
                    return new ArrayType(bytesType);
                }
                return bytesType;
            }

            if (field.isRepeated()) {
                return new ArrayType(
                        generateRowTypeInternal(field.getMessageType(), enumAsInt, ancestors));
            } else {
                return generateRowTypeInternal(field.getMessageType(), enumAsInt, ancestors);
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
