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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataInternalUtils;

import org.junit.jupiter.api.Nested;

import java.math.BigDecimal;

/** A test for the {@link DecimalDataSerializer}. */
class DecimalSerializerTest {

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
