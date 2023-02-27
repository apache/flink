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

package org.apache.flink.table.data;

import org.junit.Test;

import java.math.BigDecimal;

import static org.apache.flink.table.data.DecimalDataUtils.abs;
import static org.apache.flink.table.data.DecimalDataUtils.add;
import static org.apache.flink.table.data.DecimalDataUtils.castFrom;
import static org.apache.flink.table.data.DecimalDataUtils.castToBoolean;
import static org.apache.flink.table.data.DecimalDataUtils.castToDecimal;
import static org.apache.flink.table.data.DecimalDataUtils.castToIntegral;
import static org.apache.flink.table.data.DecimalDataUtils.ceil;
import static org.apache.flink.table.data.DecimalDataUtils.compare;
import static org.apache.flink.table.data.DecimalDataUtils.divide;
import static org.apache.flink.table.data.DecimalDataUtils.divideToIntegralValue;
import static org.apache.flink.table.data.DecimalDataUtils.doubleValue;
import static org.apache.flink.table.data.DecimalDataUtils.floor;
import static org.apache.flink.table.data.DecimalDataUtils.is32BitDecimal;
import static org.apache.flink.table.data.DecimalDataUtils.is64BitDecimal;
import static org.apache.flink.table.data.DecimalDataUtils.isByteArrayDecimal;
import static org.apache.flink.table.data.DecimalDataUtils.mod;
import static org.apache.flink.table.data.DecimalDataUtils.multiply;
import static org.apache.flink.table.data.DecimalDataUtils.negate;
import static org.apache.flink.table.data.DecimalDataUtils.sign;
import static org.apache.flink.table.data.DecimalDataUtils.signum;
import static org.apache.flink.table.data.DecimalDataUtils.sround;
import static org.apache.flink.table.data.DecimalDataUtils.subtract;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DecimalData}. */
public class DecimalDataTest {

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testNormal() {
        BigDecimal bigDecimal1 = new BigDecimal("13145678.90123");
        BigDecimal bigDecimal2 = new BigDecimal("1234567890.0987654321");
        // fromUnscaledBytes
        assertThat(DecimalData.fromUnscaledBytes(bigDecimal1.unscaledValue().toByteArray(), 15, 5))
                .isEqualTo(DecimalData.fromBigDecimal(bigDecimal1, 15, 5));
        assertThat(DecimalData.fromUnscaledBytes(bigDecimal2.unscaledValue().toByteArray(), 23, 10))
                .isEqualTo(DecimalData.fromBigDecimal(bigDecimal2, 23, 10));
        // toUnscaledBytes
        assertThat(
                        DecimalData.fromUnscaledBytes(
                                        bigDecimal1.unscaledValue().toByteArray(), 15, 5)
                                .toUnscaledBytes())
                .isEqualTo(bigDecimal1.unscaledValue().toByteArray());
        assertThat(
                        DecimalData.fromUnscaledBytes(
                                        bigDecimal2.unscaledValue().toByteArray(), 23, 10)
                                .toUnscaledBytes())
                .isEqualTo(bigDecimal2.unscaledValue().toByteArray());

        DecimalData decimal1 = DecimalData.fromUnscaledLong(10, 5, 0);
        DecimalData decimal2 = DecimalData.fromUnscaledLong(15, 5, 0);
        assertThat(DecimalData.fromBigDecimal(new BigDecimal(10), 5, 0).hashCode())
                .isEqualTo(decimal1.hashCode());
        assertThat(decimal1.copy()).isEqualTo(decimal1);
        assertThat(DecimalData.fromUnscaledLong(decimal1.toUnscaledLong(), 5, 0))
                .isEqualTo(decimal1);
        assertThat(DecimalData.fromUnscaledBytes(decimal1.toUnscaledBytes(), 5, 0))
                .isEqualTo(decimal1);
        assertThat(decimal1.compareTo(decimal2)).isLessThan(0);
        assertThat(signum(decimal1)).isEqualTo(1);
        assertThat(doubleValue(castFrom(10.5, 5, 1))).isEqualTo(10.5);
        assertThat(negate(decimal1)).isEqualTo(DecimalData.fromUnscaledLong(-10, 5, 0));
        assertThat(abs(decimal1)).isEqualTo(decimal1);
        assertThat(abs(negate(decimal1))).isEqualTo(decimal1);
        assertThat(add(decimal1, decimal2, 5, 0).toUnscaledLong()).isEqualTo(25);
        assertThat(subtract(decimal1, decimal2, 5, 0).toUnscaledLong()).isEqualTo(-5);
        assertThat(multiply(decimal1, decimal2, 5, 0).toUnscaledLong()).isEqualTo(150);
        assertThat(doubleValue(divide(decimal1, decimal2, 5, 2))).isEqualTo(0.67);
        assertThat(mod(decimal1, decimal2, 5, 0)).isEqualTo(decimal1);
        assertThat(
                        divideToIntegralValue(decimal1, DecimalData.fromUnscaledLong(2, 5, 0), 5, 0)
                                .toUnscaledLong())
                .isEqualTo(5);
        assertThat(castToIntegral(decimal1)).isEqualTo(10);
        assertThat(castToBoolean(decimal1)).isTrue();
        assertThat(compare(decimal1, 10)).isEqualTo(0);
        assertThat(compare(decimal1, 5)).isGreaterThan(0);
        assertThat(sign(castFrom(5.556, 10, 5))).isEqualTo(castFrom(1.0, 10, 5));

        assertThat(DecimalData.fromBigDecimal(new BigDecimal(Long.MAX_VALUE), 5, 0)).isNull();
        assertThat(DecimalData.zero(5, 2).toBigDecimal().intValue()).isEqualTo(0);
        assertThat(DecimalData.zero(20, 2).toBigDecimal().intValue()).isEqualTo(0);

        assertThat(floor(castFrom(10.5, 5, 1))).isEqualTo(DecimalData.fromUnscaledLong(10, 5, 0));
        assertThat(ceil(castFrom(10.5, 5, 1))).isEqualTo(DecimalData.fromUnscaledLong(11, 5, 0));
        assertThat(castToDecimal(castFrom(5.0, 10, 1), 10, 2).toString()).isEqualTo("5.00");

        assertThat(castToIntegral(castFrom(5, 5, 0))).isEqualTo(5);
        assertThat(castToIntegral(castFrom("5", 5, 0))).isEqualTo(5);

        DecimalData newDecimal = castFrom(castFrom(10, 5, 2), 10, 4);
        assertThat(newDecimal.precision()).isEqualTo(10);
        assertThat(newDecimal.scale()).isEqualTo(4);

        assertThat(is32BitDecimal(6)).isTrue();
        assertThat(is64BitDecimal(11)).isTrue();
        assertThat(isByteArrayDecimal(20)).isTrue();

        assertThat(sround(castFrom(5.555, 5, 0), 1).toUnscaledLong()).isEqualTo(6);
        assertThat(sround(castFrom(5.555, 5, 3), 1).toUnscaledLong()).isEqualTo(56);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testNotCompact() {
        DecimalData decimal1 = DecimalData.fromBigDecimal(new BigDecimal(10), 20, 0);
        DecimalData decimal2 = DecimalData.fromBigDecimal(new BigDecimal(15), 20, 0);
        assertThat(DecimalData.fromBigDecimal(new BigDecimal(10), 20, 0).hashCode())
                .isEqualTo(decimal1.hashCode());
        assertThat(decimal1.copy()).isEqualTo(decimal1);
        assertThat(DecimalData.fromBigDecimal(decimal1.toBigDecimal(), 20, 0)).isEqualTo(decimal1);
        assertThat(DecimalData.fromUnscaledBytes(decimal1.toUnscaledBytes(), 20, 0))
                .isEqualTo(decimal1);
        assertThat(decimal1.compareTo(decimal2)).isLessThan(0);
        assertThat(signum(decimal1)).isEqualTo(1);
        assertThat(doubleValue(castFrom(10.5, 20, 1))).isEqualTo(10.5);
        assertThat(negate(decimal1))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(-10), 20, 0));
        assertThat(abs(decimal1)).isEqualTo(decimal1);
        assertThat(abs(negate(decimal1))).isEqualTo(decimal1);
        assertThat(add(decimal1, decimal2, 20, 0).toBigDecimal().longValue()).isEqualTo(25);
        assertThat(subtract(decimal1, decimal2, 20, 0).toBigDecimal().longValue()).isEqualTo(-5);
        assertThat(multiply(decimal1, decimal2, 20, 0).toBigDecimal().longValue()).isEqualTo(150);
        assertThat(doubleValue(divide(decimal1, decimal2, 20, 2))).isEqualTo(0.67);
        assertThat(mod(decimal1, decimal2, 20, 0)).isEqualTo(decimal1);
        assertThat(
                        divideToIntegralValue(
                                        decimal1,
                                        DecimalData.fromBigDecimal(new BigDecimal(2), 20, 0),
                                        20,
                                        0)
                                .toBigDecimal()
                                .longValue())
                .isEqualTo(5);
        assertThat(castToIntegral(decimal1)).isEqualTo(10);
        assertThat(castToBoolean(decimal1)).isTrue();
        assertThat(compare(decimal1, 10)).isEqualTo(0);
        assertThat(compare(decimal1, 5)).isGreaterThan(0);
        assertThat(compare(DecimalData.fromBigDecimal(new BigDecimal("10.5"), 20, 2), 10))
                .isGreaterThan(0);
        assertThat(sign(castFrom(5.556, 20, 5))).isEqualTo(castFrom(1.0, 20, 5));

        assertThat(DecimalData.fromBigDecimal(new BigDecimal(Long.MAX_VALUE), 5, 0)).isNull();
        assertThat(DecimalData.zero(20, 2).toBigDecimal().intValue()).isEqualTo(0);
        assertThat(DecimalData.zero(20, 2).toBigDecimal().intValue()).isEqualTo(0);

        DecimalData decimal3 = DecimalData.fromBigDecimal(new BigDecimal(10), 18, 0);
        DecimalData decimal4 = DecimalData.fromBigDecimal(new BigDecimal(15), 18, 0);
        assertThat(DecimalDataUtils.compare(subtract(decimal3, decimal4, 19, 0), -5)).isEqualTo(0);
        assertThat(DecimalDataUtils.compare(add(decimal3, decimal4, 19, 0), 25)).isEqualTo(0);
    }

    @Test
    public void testToString() {
        String val = "0.0000000000000000001";
        assertThat(castFrom(val, 39, val.length() - 2).toString()).isEqualTo(val);
        val = "123456789012345678901234567890123456789";
        assertThat(castFrom(val, 39, 0).toString()).isEqualTo(val);
    }
}
