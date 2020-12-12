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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtobufInternalUtils;

/** Protobuf function util. */
public class PbFormatUtils {
    public static String getFullJavaName(Descriptors.Descriptor descriptor, String outerProtoName) {
        if (null != descriptor.getContainingType()) {
            // nested type
            String parentJavaFullName =
                    getFullJavaName(descriptor.getContainingType(), outerProtoName);
            return parentJavaFullName + "." + descriptor.getName();
        } else {
            // top level message
            return outerProtoName + descriptor.getName();
        }
    }

    public static String getFullJavaName(
            Descriptors.EnumDescriptor enumDescriptor, String outerProtoName) {
        if (null != enumDescriptor.getContainingType()) {
            return getFullJavaName(enumDescriptor.getContainingType(), outerProtoName)
                    + "."
                    + enumDescriptor.getName();
        } else {
            return outerProtoName + enumDescriptor.getName();
        }
    }

    public static boolean isSimpleType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return true;
            default:
                return false;
        }
    }

    public static String getStrongCamelCaseJsonName(String name) {
        return ProtobufInternalUtils.underScoreToCamelCase(name, true);
    }

    public static String getOuterProtoPrefix(String name) {
        name = name.replace('$', '.');
        int index = name.lastIndexOf('.');
        if (index != -1) {
            // include dot
            return name.substring(0, index + 1);
        } else {
            return "";
        }
    }

    public static Descriptors.Descriptor getDescriptor(String className) {
        try {
            Class<?> pbClass =
                    Class.forName(className, true, Thread.currentThread().getContextClassLoader());
            return (Descriptors.Descriptor)
                    pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("get %s descriptors error!", className), e);
        }
    }

    public static boolean isRepeatedType(LogicalType type) {
        return type instanceof MapType || type instanceof ArrayType;
    }

    public static boolean isArrayType(LogicalType type) {
        return type instanceof ArrayType;
    }
}
