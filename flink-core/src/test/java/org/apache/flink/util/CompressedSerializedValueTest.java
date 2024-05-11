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
 * limitations under the License
 */

package org.apache.flink.util;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CompressedSerializedValue}. */
class CompressedSerializedValueTest {
    @Test
    void testSimpleValue() throws Exception {

        final String value = "teststring";

        CompressedSerializedValue<String> v = CompressedSerializedValue.fromObject(value);
        CompressedSerializedValue<String> copy = CommonTestUtils.createCopySerializable(v);

        assertThat(v.deserializeValue(getClass().getClassLoader())).isEqualTo(value);
        assertThat(copy.deserializeValue(getClass().getClassLoader())).isEqualTo(value);

        assertThat(copy).isEqualTo(v);
        assertThat(copy).hasSameHashCodeAs(v.hashCode());

        assertThat(v.toString()).isNotNull();
        assertThat(copy.toString()).isNotNull();

        assertThat(v.getSize()).isNotEqualTo(0);
        assertThat(copy.getByteArray()).isEqualTo(v.getByteArray());

        byte[] bytes = v.getByteArray();
        CompressedSerializedValue<String> saved =
                CompressedSerializedValue.fromBytes(Arrays.copyOf(bytes, bytes.length));
        assertThat(saved).isEqualTo(v);
        assertThat(saved.getByteArray()).isEqualTo(v.getByteArray());
    }

    @Test
    void testNullValue() {
        assertThatThrownBy(() -> CompressedSerializedValue.fromObject(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testFromNullBytes() {
        assertThatThrownBy(() -> CompressedSerializedValue.fromBytes(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testFromEmptyBytes() {
        assertThatThrownBy(() -> CompressedSerializedValue.fromBytes(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
