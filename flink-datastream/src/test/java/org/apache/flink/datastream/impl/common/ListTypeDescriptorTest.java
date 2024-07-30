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

package org.apache.flink.datastream.impl.common;

import org.apache.flink.api.common.typeinfo.TypeDescriptor;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.flink.api.common.typeinfo.descriptor.ListTypeDescriptorImpl}. */
class ListTypeDescriptorTest {

    @ParameterizedTest
    @MethodSource("listTypeDescriptors")
    void testListTypeDescriptor(TypeDescriptor<?> typeDescriptor, String expectedString)
            throws ReflectiveOperationException {
        assertThat(TypeDescriptors.list(typeDescriptor).toString()).isEqualTo(expectedString);
    }

    static Object[][] listTypeDescriptors() {
        return new Object[][] {
            {TypeDescriptors.INT, "ListTypeDescriptorImpl [listTypeInfo=List<Integer>]"},
            {TypeDescriptors.STRING, "ListTypeDescriptorImpl [listTypeInfo=List<String>]"},
            {TypeDescriptors.BOOLEAN, "ListTypeDescriptorImpl [listTypeInfo=List<Boolean>]"},
            {TypeDescriptors.LONG, "ListTypeDescriptorImpl [listTypeInfo=List<Long>]"},
            {TypeDescriptors.BYTE, "ListTypeDescriptorImpl [listTypeInfo=List<Byte>]"},
            {TypeDescriptors.SHORT, "ListTypeDescriptorImpl [listTypeInfo=List<Short>]"},
            {TypeDescriptors.DOUBLE, "ListTypeDescriptorImpl [listTypeInfo=List<Double>]"},
            {TypeDescriptors.FLOAT, "ListTypeDescriptorImpl [listTypeInfo=List<Float>]"},
            {TypeDescriptors.CHAR, "ListTypeDescriptorImpl [listTypeInfo=List<Character>]"},
        };
    }
}
