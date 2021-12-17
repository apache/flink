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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for the {@link AkkaRpcSerializedValue}. */
public class AkkaRpcSerializedValueTest extends TestLogger {

    @Test
    public void testNullValue() throws Exception {
        AkkaRpcSerializedValue serializedValue = AkkaRpcSerializedValue.valueOf(null);
        assertThat(serializedValue.getSerializedData(), nullValue());
        assertThat(serializedValue.getSerializedDataLength(), equalTo(0));
        assertThat(serializedValue.deserializeValue(getClass().getClassLoader()), nullValue());

        AkkaRpcSerializedValue otherSerializedValue = AkkaRpcSerializedValue.valueOf(null);
        assertThat(otherSerializedValue, equalTo(serializedValue));
        assertThat(otherSerializedValue.hashCode(), equalTo(serializedValue.hashCode()));

        AkkaRpcSerializedValue clonedSerializedValue = InstantiationUtil.clone(serializedValue);
        assertThat(clonedSerializedValue.getSerializedData(), nullValue());
        assertThat(clonedSerializedValue.getSerializedDataLength(), equalTo(0));
        assertThat(
                clonedSerializedValue.deserializeValue(getClass().getClassLoader()), nullValue());
        assertThat(clonedSerializedValue, equalTo(serializedValue));
        assertThat(clonedSerializedValue.hashCode(), equalTo(serializedValue.hashCode()));
    }

    @Test
    public void testNotNullValues() throws Exception {
        Set<Object> values =
                Stream.of(
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
                                BigDecimal.valueOf(Math.PI))
                        .collect(Collectors.toSet());

        Object previousValue = null;
        AkkaRpcSerializedValue previousSerializedValue = null;
        for (Object value : values) {
            AkkaRpcSerializedValue serializedValue = AkkaRpcSerializedValue.valueOf(value);
            assertThat(value.toString(), serializedValue.getSerializedData(), notNullValue());
            assertThat(value.toString(), serializedValue.getSerializedDataLength(), greaterThan(0));
            assertThat(
                    value.toString(),
                    serializedValue.deserializeValue(getClass().getClassLoader()),
                    equalTo(value));

            AkkaRpcSerializedValue otherSerializedValue = AkkaRpcSerializedValue.valueOf(value);
            assertThat(value.toString(), otherSerializedValue, equalTo(serializedValue));
            assertThat(
                    value.toString(),
                    otherSerializedValue.hashCode(),
                    equalTo(serializedValue.hashCode()));

            AkkaRpcSerializedValue clonedSerializedValue = InstantiationUtil.clone(serializedValue);
            assertThat(
                    value.toString(),
                    clonedSerializedValue.getSerializedData(),
                    equalTo(serializedValue.getSerializedData()));
            assertThat(
                    value.toString(),
                    clonedSerializedValue.deserializeValue(getClass().getClassLoader()),
                    equalTo(value));
            assertThat(value.toString(), clonedSerializedValue, equalTo(serializedValue));
            assertThat(
                    value.toString(),
                    clonedSerializedValue.hashCode(),
                    equalTo(serializedValue.hashCode()));

            if (previousValue != null && !previousValue.equals(value)) {
                assertThat(
                        value.toString() + " " + previousValue.toString(),
                        serializedValue,
                        not(equalTo(previousSerializedValue)));
            }

            previousValue = value;
            previousSerializedValue = serializedValue;
        }
    }
}
