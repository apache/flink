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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link NullableSerializer}. */
abstract class NullableSerializerTest extends SerializerTestBase<Integer> {
    private static final TypeSerializer<Integer> originalSerializer = IntSerializer.INSTANCE;

    private TypeSerializer<Integer> nullableSerializer;

    @BeforeEach
    void init() {
        nullableSerializer =
                NullableSerializer.wrapIfNullIsNotSupported(
                        originalSerializer, isPaddingNullValue());
    }

    @Override
    protected TypeSerializer<Integer> createSerializer() {
        return NullableSerializer.wrapIfNullIsNotSupported(
                originalSerializer, isPaddingNullValue());
    }

    @Override
    protected int getLength() {
        return isPaddingNullValue() ? 5 : -1;
    }

    @Override
    protected Class<Integer> getTypeClass() {
        return Integer.class;
    }

    @Override
    protected Integer[] getTestData() {
        return new Integer[] {5, -1, null, 5};
    }

    @Test
    void testWrappingNotNeeded() {
        assertThat(
                        NullableSerializer.wrapIfNullIsNotSupported(
                                StringSerializer.INSTANCE, isPaddingNullValue()))
                .isEqualTo(StringSerializer.INSTANCE);
    }

    @Test
    void testWrappingNeeded() {
        assertThat(nullableSerializer)
                .isInstanceOf(NullableSerializer.class)
                .isEqualTo(
                        NullableSerializer.wrapIfNullIsNotSupported(
                                nullableSerializer, isPaddingNullValue()));
    }

    abstract boolean isPaddingNullValue();

    static final class NullableSerializerWithPaddingTest extends NullableSerializerTest {

        @Override
        boolean isPaddingNullValue() {
            return true;
        }
    }

    static final class NullableSerializerWithoutPaddingTest extends NullableSerializerTest {

        @Override
        boolean isPaddingNullValue() {
            return false;
        }
    }
}
