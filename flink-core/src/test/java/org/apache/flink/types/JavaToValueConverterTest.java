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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JavaToValueConverterTest {

    @Test
    void testJavaToValueConversion() {
        assertThat(JavaToValueConverter.convertBoxedJavaType(null)).isNull();

        assertThat(JavaToValueConverter.convertBoxedJavaType("123Test"))
                .isEqualTo(new StringValue("123Test"));
        assertThat(JavaToValueConverter.convertBoxedJavaType((byte) 44))
                .isEqualTo(new ByteValue((byte) 44));
        assertThat(JavaToValueConverter.convertBoxedJavaType((short) 10000))
                .isEqualTo(new ShortValue((short) 10000));
        assertThat(JavaToValueConverter.convertBoxedJavaType(3567564))
                .isEqualTo(new IntValue(3567564));
        assertThat(JavaToValueConverter.convertBoxedJavaType(767692734L))
                .isEqualTo(new LongValue(767692734));
        assertThat(JavaToValueConverter.convertBoxedJavaType(17.5f))
                .isEqualTo(new FloatValue(17.5f));
        assertThat(JavaToValueConverter.convertBoxedJavaType(3.1415926))
                .isEqualTo(new DoubleValue(3.1415926));
        assertThat(JavaToValueConverter.convertBoxedJavaType(true))
                .isEqualTo(new BooleanValue(true));
        assertThat(JavaToValueConverter.convertBoxedJavaType('@')).isEqualTo(new CharValue('@'));

        assertThatThrownBy(() -> JavaToValueConverter.convertBoxedJavaType(new ArrayList<>()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testValueToJavaConversion() {
        assertThat(JavaToValueConverter.convertValueType(null)).isNull();

        assertThat(JavaToValueConverter.convertValueType(new StringValue("123Test")))
                .isEqualTo("123Test");
        assertThat(JavaToValueConverter.convertValueType(new ByteValue((byte) 44)))
                .isEqualTo((byte) 44);
        assertThat(JavaToValueConverter.convertValueType(new ShortValue((short) 10000)))
                .isEqualTo((short) 10000);
        assertThat(JavaToValueConverter.convertValueType(new IntValue(3567564))).isEqualTo(3567564);
        assertThat(JavaToValueConverter.convertValueType(new LongValue(767692734)))
                .isEqualTo(767692734L);
        assertThat(JavaToValueConverter.convertValueType(new FloatValue(17.5f))).isEqualTo(17.5f);
        assertThat(JavaToValueConverter.convertValueType(new DoubleValue(3.1415926)))
                .isEqualTo(3.1415926);
        assertThat((Boolean) JavaToValueConverter.convertValueType(new BooleanValue(true)))
                .isTrue();
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
