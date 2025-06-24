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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RpcSerializedValue}. */
class RpcSerializedValueTest {

    @Test
    void testNullValue() throws Exception {
        RpcSerializedValue serializedValue = RpcSerializedValue.valueOf(null);
        assertThat(serializedValue.getSerializedData()).isNull();
        assertThat(serializedValue.getSerializedDataLength()).isEqualTo(0);
        assertThat((Object) serializedValue.deserializeValue(getClass().getClassLoader())).isNull();

        RpcSerializedValue otherSerializedValue = RpcSerializedValue.valueOf(null);
        assertThat(otherSerializedValue).isEqualTo(serializedValue);
        assertThat(otherSerializedValue.hashCode()).isEqualTo(serializedValue.hashCode());

        RpcSerializedValue clonedSerializedValue = InstantiationUtil.clone(serializedValue);
        assertThat(clonedSerializedValue.getSerializedData()).isNull();
        assertThat(clonedSerializedValue.getSerializedDataLength()).isEqualTo(0);
        assertThat((Object) clonedSerializedValue.deserializeValue(getClass().getClassLoader()))
                .isNull();
        assertThat(clonedSerializedValue).isEqualTo(serializedValue);
        assertThat(clonedSerializedValue.hashCode()).isEqualTo(serializedValue.hashCode());
    }

    static Stream<Object> serializationArguments() {
        return Stream.of(
                true,
                (byte) 5,
                (short) 6,
                5,
                5L,
                5.5F,
                6.5,
                'c',
                "string",
                Instant.now(),
                BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN),
                BigDecimal.valueOf(Math.PI));
    }

    @ParameterizedTest
    @MethodSource("serializationArguments")
    void testNotNullValues(Object value) throws Exception {
        RpcSerializedValue serializedValue = RpcSerializedValue.valueOf(value);
        assertThat(serializedValue.getSerializedData()).isNotNull();
        assertThat(serializedValue.getSerializedDataLength()).isGreaterThan(0);
        assertThat((Object) serializedValue.deserializeValue(getClass().getClassLoader()))
                .isEqualTo(value);

        RpcSerializedValue otherSerializedValue = RpcSerializedValue.valueOf(value);
        assertThat(otherSerializedValue).isEqualTo(serializedValue);
        assertThat(otherSerializedValue.hashCode()).isEqualTo(serializedValue.hashCode());

        RpcSerializedValue clonedSerializedValue = InstantiationUtil.clone(serializedValue);
        assertThat(clonedSerializedValue.getSerializedData())
                .isEqualTo(serializedValue.getSerializedData());
        assertThat((Object) clonedSerializedValue.deserializeValue(getClass().getClassLoader()))
                .isEqualTo(value);
        assertThat(clonedSerializedValue).isEqualTo(serializedValue);
        assertThat(clonedSerializedValue.hashCode()).isEqualTo(serializedValue.hashCode());
    }
}
