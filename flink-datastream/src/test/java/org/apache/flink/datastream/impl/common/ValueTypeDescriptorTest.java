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
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.flink.api.common.typeinfo.descriptor.ValueTypeDescriptorImpl}. */
class ValueTypeDescriptorTest {

    @ParameterizedTest
    @MethodSource("valueTypeDescriptors")
    <T> void testValueTypeDescriptor(TypeDescriptor<T> typeDescriptor, String expectedString)
            throws ReflectiveOperationException {
        assertThat(TypeDescriptors.value(typeDescriptor).toString()).isEqualTo(expectedString);
    }

    static Object[][] valueTypeDescriptors() {
        return new Object[][] {
            {
                (TypeDescriptor<BooleanValue>) () -> BooleanValue.class,
                "ValueTypeDescriptorImpl [valueTypeInfo=ValueType<BooleanValue>]"
            },
            {
                (TypeDescriptor<IntValue>) () -> IntValue.class,
                "ValueTypeDescriptorImpl [valueTypeInfo=ValueType<IntValue>]"
            },
            {
                (TypeDescriptor<LongValue>) () -> LongValue.class,
                "ValueTypeDescriptorImpl [valueTypeInfo=ValueType<LongValue>]"
            },
            {
                (TypeDescriptor<ShortValue>) () -> ShortValue.class,
                "ValueTypeDescriptorImpl [valueTypeInfo=ValueType<ShortValue>]"
            },
            {
                (TypeDescriptor<FloatValue>) () -> FloatValue.class,
                "ValueTypeDescriptorImpl [valueTypeInfo=ValueType<FloatValue>]"
            },
            {
                (TypeDescriptor<CharValue>) () -> CharValue.class,
                "ValueTypeDescriptorImpl [valueTypeInfo=ValueType<CharValue>]"
            },
            {
                (TypeDescriptor<DoubleValue>) () -> DoubleValue.class,
                "ValueTypeDescriptorImpl [valueTypeInfo=ValueType<DoubleValue>]"
            },
        };
    }

    @Test
    void testValueTypeDescriptorNonValue() {
        assertThatThrownBy(
                        () ->
                                TypeDescriptors.value((TypeDescriptor<Double>) () -> Double.class)
                                        .toString())
                .isInstanceOf(InvocationTargetException.class);
    }
}
