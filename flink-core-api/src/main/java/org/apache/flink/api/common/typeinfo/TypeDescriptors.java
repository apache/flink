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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.utils.TypeUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Descriptor interface to create TypeInformation instances. */
@Experimental
public class TypeDescriptors implements Serializable {
    @SuppressWarnings("unchecked")
    public static <T> TypeDescriptor<T> value(TypeDescriptor<T> typeDescriptor)
            throws ReflectiveOperationException {

        return (TypeDescriptor<T>)
                TypeUtils.getInstance(
                        "org.apache.flink.api.common.typeinfo.descriptor.ValueTypeDescriptorImpl",
                        typeDescriptor);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> TypeDescriptor<Map<K, V>> map(
            TypeDescriptor<K> keyTypeDescriptor, TypeDescriptor<V> valueTypeDescriptor)
            throws ReflectiveOperationException {

        return (TypeDescriptor<Map<K, V>>)
                TypeUtils.getInstance(
                        "org.apache.flink.api.common.typeinfo.descriptor.MapTypeDescriptorImpl",
                        keyTypeDescriptor,
                        valueTypeDescriptor);
    }

    @SuppressWarnings("unchecked")
    public static <T> TypeDescriptor<List<T>> list(TypeDescriptor<T> elementTypeDescriptor)
            throws ReflectiveOperationException {

        return (TypeDescriptor<List<T>>)
                TypeUtils.getInstance(
                        "org.apache.flink.api.common.typeinfo.descriptor.ListTypeDescriptorImpl",
                        elementTypeDescriptor);
    }

    // BasicTypeInfo type descriptors
    public static final TypeDescriptor<String> STRING;
    public static final TypeDescriptor<Integer> INT;
    public static final TypeDescriptor<Boolean> BOOLEAN;
    public static final TypeDescriptor<Long> LONG;
    public static final TypeDescriptor<Byte> BYTE;
    public static final TypeDescriptor<Short> SHORT;
    public static final TypeDescriptor<Double> DOUBLE;
    public static final TypeDescriptor<Float> FLOAT;
    public static final TypeDescriptor<Character> CHAR;

    static {
        try {
            @SuppressWarnings("unchecked")
            TypeDescriptor<String> stringTypeTemp =
                    (TypeDescriptor<String>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<String>) () -> String.class);
            STRING = stringTypeTemp;

            @SuppressWarnings("unchecked")
            TypeDescriptor<Integer> intTypeTemp =
                    (TypeDescriptor<Integer>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<Integer>) () -> Integer.class);
            INT = intTypeTemp;

            @SuppressWarnings("unchecked")
            TypeDescriptor<Boolean> booleanTypeTemp =
                    (TypeDescriptor<Boolean>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<Boolean>) () -> Boolean.class);
            BOOLEAN = booleanTypeTemp;

            @SuppressWarnings("unchecked")
            TypeDescriptor<Long> longTypeTemp =
                    (TypeDescriptor<Long>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<Long>) () -> Long.class);
            LONG = longTypeTemp;

            @SuppressWarnings("unchecked")
            TypeDescriptor<Byte> byteTypeTemp =
                    (TypeDescriptor<Byte>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<Byte>) () -> Byte.class);
            BYTE = byteTypeTemp;

            @SuppressWarnings("unchecked")
            TypeDescriptor<Short> shortTypeTemp =
                    (TypeDescriptor<Short>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<Short>) () -> Short.class);
            SHORT = shortTypeTemp;

            @SuppressWarnings("unchecked")
            TypeDescriptor<Double> doubleTypeTemp =
                    (TypeDescriptor<Double>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<Double>) () -> Double.class);
            DOUBLE = doubleTypeTemp;

            @SuppressWarnings("unchecked")
            TypeDescriptor<Float> floatTypeTemp =
                    (TypeDescriptor<Float>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<Float>) () -> Float.class);
            FLOAT = floatTypeTemp;

            @SuppressWarnings("unchecked")
            TypeDescriptor<Character> charTypeTemp =
                    (TypeDescriptor<Character>)
                            TypeUtils.getInstance(
                                    "org.apache.flink.api.common.typeinfo.descriptor.BasicTypeDescriptorImpl",
                                    (TypeDescriptor<Character>) () -> Character.class);
            CHAR = charTypeTemp;

        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
