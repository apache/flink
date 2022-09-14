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

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class JavaToValueConverterTest {

    @Test
    public void testJavaToValueConversion() {
        try {
            assertNull(JavaToValueConverter.convertBoxedJavaType(null));

            assertEquals(
                    new StringValue("123Test"),
                    JavaToValueConverter.convertBoxedJavaType("123Test"));
            assertEquals(
                    new ByteValue((byte) 44), JavaToValueConverter.convertBoxedJavaType((byte) 44));
            assertEquals(
                    new ShortValue((short) 10000),
                    JavaToValueConverter.convertBoxedJavaType((short) 10000));
            assertEquals(new IntValue(3567564), JavaToValueConverter.convertBoxedJavaType(3567564));
            assertEquals(
                    new LongValue(767692734),
                    JavaToValueConverter.convertBoxedJavaType(767692734L));
            assertEquals(new FloatValue(17.5f), JavaToValueConverter.convertBoxedJavaType(17.5f));
            assertEquals(
                    new DoubleValue(3.1415926),
                    JavaToValueConverter.convertBoxedJavaType(3.1415926));
            assertEquals(new BooleanValue(true), JavaToValueConverter.convertBoxedJavaType(true));
            assertEquals(new CharValue('@'), JavaToValueConverter.convertBoxedJavaType('@'));

            try {
                JavaToValueConverter.convertBoxedJavaType(new ArrayList<Object>());
                fail("Accepted invalid type.");
            } catch (IllegalArgumentException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testValueToJavaConversion() {
        try {
            assertNull(JavaToValueConverter.convertValueType(null));

            assertEquals(
                    "123Test", JavaToValueConverter.convertValueType(new StringValue("123Test")));
            assertEquals(
                    (byte) 44, JavaToValueConverter.convertValueType(new ByteValue((byte) 44)));
            assertEquals(
                    (short) 10000,
                    JavaToValueConverter.convertValueType(new ShortValue((short) 10000)));
            assertEquals(3567564, JavaToValueConverter.convertValueType(new IntValue(3567564)));
            assertEquals(
                    767692734L, JavaToValueConverter.convertValueType(new LongValue(767692734)));
            assertEquals(17.5f, JavaToValueConverter.convertValueType(new FloatValue(17.5f)));
            assertEquals(
                    3.1415926, JavaToValueConverter.convertValueType(new DoubleValue(3.1415926)));
            assertEquals(true, JavaToValueConverter.convertValueType(new BooleanValue(true)));
            assertEquals('@', JavaToValueConverter.convertValueType(new CharValue('@')));

            try {
                JavaToValueConverter.convertValueType(new MyValue());
                fail("Accepted invalid type.");
            } catch (IllegalArgumentException e) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private static final class MyValue implements Value {
        private static final long serialVersionUID = 1L;

        @Override
        public void write(DataOutputView out) {}

        @Override
        public void read(DataInputView in) {}
    }
}
