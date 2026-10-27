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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataInternalUtils;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link DecimalDataSerializer}. */
class DecimalSerializerTest {

    /** Bytes as written by the BigDecimal-only serializer, which must stay readable. */
    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
        "00, 0",
        "7f, 127",
        "0080, 128",
        "80, -128",
        "ff7f, -129",
        "7fffffffffffffff, 9223372036854775807",
        "8000000000000000, -9223372036854775808",
        "008000000000000000, 9223372036854775808",
        "ff7fffffffffffffff, -9223372036854775809"
    })
    void testDeserializeLegacyNonCompactBytes(String hex, String unscaled) throws IOException {
        DecimalDataSerializer serializer = new DecimalDataSerializer(38, 2);
        byte[] bytes = StringUtils.hexStringToByte(hex);
        BigDecimal expected = new BigDecimal(new BigInteger(unscaled), 2);

        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeInt(bytes.length);
        out.write(bytes);
        byte[] serialized = out.getCopyOfBuffer();

        DecimalData decimal = serializer.deserialize(new DataInputDeserializer(serialized));
        assertThat(decimal.toBigDecimal()).isEqualTo(expected);
        assertThat(decimal.hashCode()).isEqualTo(expected.hashCode());

        out.clear();
        serializer.serialize(decimal, out);
        assertThat(out.getCopyOfBuffer()).isEqualTo(serialized);
    }

    @Nested
    final class CompactDecimalSerializerTest extends DecimalSerializerTestBase {
        @Override
        protected DecimalDataSerializer createSerializer() {
            return new DecimalDataSerializer(5, 2);
        }

        @Override
        protected DecimalData[] getTestData() {
            return new DecimalData[] {
                DecimalData.fromUnscaledLong(1, 5, 2),
                DecimalData.fromUnscaledLong(2, 5, 2),
                DecimalData.fromUnscaledLong(3, 5, 2),
                DecimalData.fromUnscaledLong(4, 5, 2)
            };
        }
    }

    @Nested
    final class NonCompactDecimalSerializerTest extends DecimalSerializerTestBase {
        @Override
        protected DecimalDataSerializer createSerializer() {
            return new DecimalDataSerializer(38, 2);
        }

        @Override
        protected DecimalData[] getTestData() {
            return new DecimalData[] {
                DecimalDataInternalUtils.fromUnscaledLong(0, 38, 2),
                DecimalDataInternalUtils.fromUnscaledLong(-1, 38, 2),
                DecimalDataInternalUtils.fromUnscaledLong(128, 38, 2),
                DecimalDataInternalUtils.fromUnscaledLong(-129, 38, 2),
                DecimalDataInternalUtils.fromUnscaledLong(Long.MAX_VALUE, 38, 2),
                DecimalDataInternalUtils.fromUnscaledLong(Long.MIN_VALUE, 38, 2),
                DecimalData.fromBigDecimal(new BigDecimal("12.34"), 38, 2),
                DecimalData.fromBigDecimal(new BigDecimal("92233720368547758.08"), 38, 2),
                DecimalData.fromBigDecimal(
                        new BigDecimal("-123456789012345678901234567890.12"), 38, 2)
            };
        }
    }

    abstract static class DecimalSerializerTestBase extends SerializerTestBase<DecimalData> {

        @Override
        protected int getLength() {
            return -1;
        }

        @Override
        protected Class<DecimalData> getTypeClass() {
            return DecimalData.class;
        }
    }
}
