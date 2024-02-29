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

package org.apache.flink.types;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JavaToValueConverterTest {

    @Test
    public void testJavaToValueConversion() {
        assertThat(JavaToValueConverter.convertBoxedJavaType(null)).isNull();

        Assertions.assertEquals(
                new StringValue("123Test"), JavaToValueConverter.convertBoxedJavaType("123Test"));
        Assertions.assertEquals(
                new ByteValue((byte) 44), JavaToValueConverter.convertBoxedJavaType((byte) 44));
        Assertions.assertEquals(
                new ShortValue((short) 10000),
                JavaToValueConverter.convertBoxedJavaType((short) 10000));
        assertThat(JavaToValueConverter.convertBoxedJavaType(3567564))
                .isEqualTo(new IntValue(3567564));
        Assertions.assertEquals(
                new LongValue(767692734), JavaToValueConverter.convertBoxedJavaType(767692734L));
        assertThat(JavaToValueConverter.convertBoxedJavaType(17.5f))
                .isEqualTo(new FloatValue(17.5f));
        Assertions.assertEquals(
                new DoubleValue(3.1415926), JavaToValueConverter.convertBoxedJavaType(3.1415926));
        assertThat(JavaToValueConverter.convertBoxedJavaType(true))
                .isEqualTo(new BooleanValue(true));
        assertThat(JavaToValueConverter.convertBoxedJavaType('@')).isEqualTo(new CharValue('@'));

        assertThatThrownBy(() -> JavaToValueConverter.convertBoxedJavaType(new ArrayList<>()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testValueToJavaConversion() {
        assertThat(JavaToValueConverter.convertValueType(null)).isNull();

        Assertions.assertEquals(
                "123Test", JavaToValueConverter.convertValueType(new StringValue("123Test")));
        Assertions.assertEquals(
                (byte) 44, JavaToValueConverter.convertValueType(new ByteValue((byte) 44)));
        Assertions.assertEquals(
                (short) 10000,
                JavaToValueConverter.convertValueType(new ShortValue((short) 10000)));
        assertThat(JavaToValueConverter.convertValueType(new IntValue(3567564))).isEqualTo(3567564);
        Assertions.assertEquals(
                767692734L, JavaToValueConverter.convertValueType(new LongValue(767692734)));
        assertThat(JavaToValueConverter.convertValueType(new FloatValue(17.5f))).isEqualTo(17.5f);
        Assertions.assertEquals(
                3.1415926, JavaToValueConverter.convertValueType(new DoubleValue(3.1415926)));
        assertThat(JavaToValueConverter.convertValueType(new BooleanValue(true))).isEqualTo(true);
        assertThat(JavaToValueConverter.convertValueType(new CharValue('@'))).isEqualTo('@');

        assertThatThrownBy(() -> JavaToValueConverter.convertValueType(new MyValue()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static final class MyValue implements Value {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(DataOutputView out) {}

        @Override
        public void read(DataInputView in) {}
    }
}
