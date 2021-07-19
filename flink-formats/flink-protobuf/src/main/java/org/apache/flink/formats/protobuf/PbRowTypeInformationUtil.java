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

package org.apache.flink.formats.protobuf;

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

/** Generate Row type information according to pb descriptors. */
public class PbRowTypeInformationUtil {
    public static RowType generateRowType(Descriptors.Descriptor root) {
        int size = root.getFields().size();
        LogicalType[] types = new LogicalType[size];
        String[] rowFieldNames = new String[size];

        for (int i = 0; i < size; i++) {
            FieldDescriptor field = root.getFields().get(i);
            rowFieldNames[i] = field.getName();
            types[i] = generateFieldTypeInformation(field);
        }
        return RowType.of(types, rowFieldNames);
    }

    public static LogicalType generateFieldTypeInformation(FieldDescriptor field) {
        JavaType fieldType = field.getJavaType();
        LogicalType type;
        if (fieldType.equals(JavaType.MESSAGE)) {
            if (field.isMapField()) {
                MapType mapType =
                        new MapType(
                                generateFieldTypeInformation(
                                        field.getMessageType()
                                                .findFieldByName(PbConstant.PB_MAP_KEY_NAME)),
                                generateFieldTypeInformation(
                                        field.getMessageType()
                                                .findFieldByName(PbConstant.PB_MAP_VALUE_NAME)));
                return mapType;
            } else if (field.isRepeated()) {
                return new ArrayType(generateRowType(field.getMessageType()));
            } else {
                return generateRowType(field.getMessageType());
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
                type = new VarCharType(Integer.MAX_VALUE);
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
