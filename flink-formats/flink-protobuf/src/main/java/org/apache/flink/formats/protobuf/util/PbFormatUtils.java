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
    public static String getFullJavaName(Descriptors.Descriptor descriptor) {
        if (null != descriptor.getContainingType()) {
            // nested type
            String parentJavaFullName = getFullJavaName(descriptor.getContainingType());
            return parentJavaFullName + "." + descriptor.getName();
        } else {
            // top level message
            String outerProtoName = getOuterProtoPrefix(descriptor.getFile());
            return outerProtoName + descriptor.getName();
        }
    }

    public static String getFullJavaName(Descriptors.EnumDescriptor enumDescriptor) {
        if (null != enumDescriptor.getContainingType()) {
            return getFullJavaName(enumDescriptor.getContainingType())
                    + "."
                    + enumDescriptor.getName();
        } else {
            String outerProtoName = getOuterProtoPrefix(enumDescriptor.getFile());
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

    public static String getOuterClassName(Descriptors.FileDescriptor fileDescriptor) {
        if (fileDescriptor.getOptions().hasJavaOuterClassname()) {
            return fileDescriptor.getOptions().getJavaOuterClassname();
        } else {
            String[] fileNames = fileDescriptor.getName().split("/");
            String fileName = fileNames[fileNames.length - 1];
            String outerName = getStrongCamelCaseJsonName(fileName.split("\\.")[0]);
            // https://developers.google.com/protocol-buffers/docs/reference/java-generated#invocation
            // The name of the wrapper class is determined by converting the base name of the .proto
            // file to camel case if the java_outer_classname option is not specified.
            // For example, foo_bar.proto produces the class name FooBar. If there is a service,
            // enum, or message (including nested types) in the file with the same name,
            // "OuterClass" will be appended to the wrapper class's name.
            boolean hasSameNameMessage =
                    fileDescriptor.getMessageTypes().stream()
                            .anyMatch(f -> f.getName().equals(outerName));
            boolean hasSameNameEnum =
                    fileDescriptor.getEnumTypes().stream()
                            .anyMatch(f -> f.getName().equals(outerName));
            boolean hasSameNameService =
                    fileDescriptor.getServices().stream()
                            .anyMatch(f -> f.getName().equals(outerName));
            if (hasSameNameMessage || hasSameNameEnum || hasSameNameService) {
                return outerName + PbConstant.PB_OUTER_CLASS_SUFFIX;
            } else {
                return outerName;
            }
        }
    }

    public static String getOuterProtoPrefix(Descriptors.FileDescriptor fileDescriptor) {
        String javaPackageName =
                fileDescriptor.getOptions().hasJavaPackage()
                        ? fileDescriptor.getOptions().getJavaPackage()
                        : fileDescriptor.getPackage();
        if (fileDescriptor.getOptions().getJavaMultipleFiles()) {
            return javaPackageName + ".";
        } else {
            String outerClassName = getOuterClassName(fileDescriptor);
            return javaPackageName + "." + outerClassName + ".";
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
