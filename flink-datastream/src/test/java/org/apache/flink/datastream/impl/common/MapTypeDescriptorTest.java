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

/** Tests for {@link org.apache.flink.api.common.typeinfo.descriptor.MapTypeDescriptorImpl}. */
class MapTypeDescriptorTest {

    @ParameterizedTest
    @MethodSource("mapTypeDescriptors")
    void testMapTypeDescriptor(
            TypeDescriptor<?> keyDescriptor,
            TypeDescriptor<?> valueDescriptor,
            String expectedString)
            throws ReflectiveOperationException {
        assertThat(TypeDescriptors.map(keyDescriptor, valueDescriptor).toString())
                .isEqualTo(expectedString);
    }

    static Object[][] mapTypeDescriptors() throws ReflectiveOperationException {
        return new Object[][] {
            {
                TypeDescriptors.INT,
                TypeDescriptors.STRING,
                "MapTypeDescriptorImplMap<Integer, String>"
            },
            {
                TypeDescriptors.BOOLEAN,
                TypeDescriptors.LONG,
                "MapTypeDescriptorImplMap<Boolean, Long>"
            },
            {
                TypeDescriptors.CHAR,
                TypeDescriptors.DOUBLE,
                "MapTypeDescriptorImplMap<Character, Double>"
            },
            {
                TypeDescriptors.SHORT,
                TypeDescriptors.FLOAT,
                "MapTypeDescriptorImplMap<Short, Float>"
            },
            {
                TypeDescriptors.CHAR,
                TypeDescriptors.list(TypeDescriptors.STRING),
                "MapTypeDescriptorImplMap<Character, GenericType<java.util.List>>"
            }
        };
    }
}
