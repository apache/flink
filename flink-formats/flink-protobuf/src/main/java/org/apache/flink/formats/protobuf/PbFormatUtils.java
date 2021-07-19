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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.Descriptors;
import org.apache.commons.lang3.StringUtils;

/** Protobuf function util. */
public class PbFormatUtils {

    /**
     * protobuf code has a bug that, f_abc_7d will be convert to fAbc7d in {@code
     * com.google.protobuf.Descriptors.FileDescriptor.getJsonName()}, but actually we need fAbc7D.
     */
    public static String fieldNameToJsonName(String name) {
        final int length = name.length();
        StringBuilder result = new StringBuilder(length);
        boolean isNextUpperCase = false;
        for (int i = 0; i < length; i++) {
            char ch = name.charAt(i);
            if (ch == '_') {
                isNextUpperCase = true;
            } else if (isNextUpperCase) {
                if ('a' <= ch && ch <= 'z') {
                    ch = (char) (ch - 'a' + 'A');
                    isNextUpperCase = false;
                }
                result.append(ch);
            } else {
                result.append(ch);
            }
        }
        return result.toString();
    }

    private static String getJavaPackageFromProtoFile(Descriptors.Descriptor descriptor) {
        boolean hasJavaPackage = descriptor.getFile().getOptions().hasJavaPackage();
        if (hasJavaPackage) {
            String javaPackage = descriptor.getFile().getOptions().getJavaPackage();
            if (StringUtils.isBlank(javaPackage)) {
                throw new FlinkRuntimeException("java_package cannot be blank string");
            }
            return javaPackage;
        } else {
            String packageName = descriptor.getFile().getPackage();
            if (StringUtils.isBlank(packageName)) {
                throw new FlinkRuntimeException("package and java_package cannot both be empty");
            }
            return packageName;
        }
    }

    public static String getFullJavaName(Descriptors.Descriptor descriptor) {
        String javaPackageName = getJavaPackageFromProtoFile(descriptor);
        if (descriptor.getFile().getOptions().getJavaMultipleFiles()) {
            // multiple_files=true
            if (null != descriptor.getContainingType()) {
                // nested type
                String parentJavaFullName = getFullJavaName(descriptor.getContainingType());
                return parentJavaFullName + "." + descriptor.getName();
            } else {
                // top level message
                return javaPackageName + "." + descriptor.getName();
            }
        } else {
            // multiple_files=false
            if (null != descriptor.getContainingType()) {
                // nested type
                String parentJavaFullName = getFullJavaName(descriptor.getContainingType());
                return parentJavaFullName + "." + descriptor.getName();
            } else {
                // top level message
                if (!descriptor.getFile().getOptions().hasJavaOuterClassname()) {
                    // user do not define outer class name in proto file
                    return javaPackageName
                            + "."
                            + descriptor.getName()
                            + PbConstant.PB_OUTER_CLASS_SUFFIX
                            + "."
                            + descriptor.getName();

                } else {
                    String outerName = descriptor.getFile().getOptions().getJavaOuterClassname();
                    // user define outer class name in proto file
                    return javaPackageName + "." + outerName + "." + descriptor.getName();
                }
            }
        }
    }

    public static String getFullJavaName(Descriptors.EnumDescriptor enumDescriptor) {
        if (null != enumDescriptor.getContainingType()) {
            return getFullJavaName(enumDescriptor.getContainingType())
                    + "."
                    + enumDescriptor.getName();
        } else {
            return enumDescriptor.getFullName();
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
        String jsonName = fieldNameToJsonName(name);
        if (jsonName.length() == 1) {
            return jsonName.toUpperCase();
        } else {
            return jsonName.substring(0, 1).toUpperCase() + jsonName.substring(1);
        }
    }

    public static Descriptors.Descriptor getDescriptor(String className) {
        try {
            Class<?> pbClass = Class.forName(className);
            return (Descriptors.Descriptor)
                    pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
        } catch (Exception y) {
            throw new IllegalArgumentException(
                    String.format("get %s descriptors error!", className), y);
        }
    }

    public static boolean isRepeatedType(LogicalType type) {
        return type instanceof MapType || type instanceof ArrayType;
    }

    public static boolean isArrayType(LogicalType type) {
        return type instanceof ArrayType;
    }
}
