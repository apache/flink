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

package org.apache.flink.table.dataformat;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

/**
 * Test for {@link Decimal}.
 */
public class DecimalTest {

	@Test
	public void testNormal() {
		Decimal decimal1 = Decimal.fromLong(10, 5, 0);
		Decimal decimal2 = Decimal.fromLong(15, 5, 0);
		Assert.assertEquals(decimal1.hashCode(),
				Decimal.fromBigDecimal(new BigDecimal(10), 5, 0).hashCode());
		Assert.assertEquals(decimal1, decimal1.copy());
		Assert.assertEquals(decimal1, Decimal.fromUnscaledLong(5, 0, decimal1.toUnscaledLong()));
		Assert.assertEquals(decimal1, Decimal.fromUnscaledBytes(5, 0, decimal1.toUnscaledBytes()));
		Assert.assertTrue(decimal1.compareTo(decimal2) < 0);
		Assert.assertEquals(1, decimal1.signum());
		Assert.assertTrue(10.5 == Decimal.castFrom(10.5, 5, 1).doubleValue());
		Assert.assertEquals(Decimal.fromLong(-10, 5, 0), decimal1.negate());
		Assert.assertEquals(decimal1, decimal1.abs());
		Assert.assertEquals(decimal1, decimal1.negate().abs());
		Assert.assertEquals(25, Decimal.add(decimal1, decimal2, 5, 0).toUnscaledLong());
		Assert.assertEquals(-5, Decimal.subtract(decimal1, decimal2, 5, 0).toUnscaledLong());
		Assert.assertEquals(150, Decimal.multiply(decimal1, decimal2, 5, 0).toUnscaledLong());
		Assert.assertTrue(0.67 == Decimal.divide(decimal1, decimal2, 5, 2).doubleValue());
		Assert.assertEquals(decimal1, Decimal.mod(decimal1, decimal2, 5, 0));
		Assert.assertEquals(5, Decimal.divideToIntegralValue(
				decimal1, Decimal.fromLong(2, 5, 0), 5, 0).toUnscaledLong());
		Assert.assertEquals(10, Decimal.castToIntegral(decimal1));
		Assert.assertEquals(true, Decimal.castToBoolean(decimal1));
		Assert.assertTrue(Decimal.compare(decimal1, 10) == 0);
		Assert.assertTrue(Decimal.compare(decimal1, 5) > 0);
		Assert.assertEquals(Decimal.castFrom(1.0, 10, 5), Decimal.sign(Decimal.castFrom(5.556, 10, 5)));

		Assert.assertNull(Decimal.fromBigDecimal(new BigDecimal(Long.MAX_VALUE), 5, 0));
		Assert.assertEquals(0, Decimal.zero(5, 2).toBigDecimal().intValue());
		Assert.assertEquals(0, Decimal.zero(20, 2).toBigDecimal().intValue());

		Assert.assertEquals(Decimal.fromLong(10, 5, 0), Decimal.castFrom(10.5, 5, 1).floor());
		Assert.assertEquals(Decimal.fromLong(11, 5, 0), Decimal.castFrom(10.5, 5, 1).ceil());
		Assert.assertEquals("5.00", Decimal.castToDecimal(Decimal.castFrom(5.0, 10, 1), 10, 2).toString());

		Assert.assertEquals(true, Decimal.castToBoolean(Decimal.castFrom(true, 5, 0)));
		Assert.assertEquals(5, Decimal.castToIntegral(Decimal.castFrom(5, 5, 0)));
		Assert.assertEquals(5, Decimal.castToIntegral(Decimal.castFrom("5", 5, 0)));
		Assert.assertEquals(5000, Decimal.castToTimestamp(Decimal.castFrom("5", 5, 0)));

		Decimal newDecimal = Decimal.castFrom(Decimal.castFrom(10, 5, 2), 10, 4);
		Assert.assertEquals(10, newDecimal.getPrecision());
		Assert.assertEquals(4, newDecimal.getScale());

		Assert.assertEquals(true, Decimal.is32BitDecimal(6));
		Assert.assertEquals(true, Decimal.is64BitDecimal(11));
		Assert.assertEquals(true, Decimal.isByteArrayDecimal(20));

		Assert.assertEquals(6, Decimal.sround(Decimal.castFrom(5.555, 5, 0), 1).toUnscaledLong());
		Assert.assertEquals(56, Decimal.sround(Decimal.castFrom(5.555, 5, 3), 1).toUnscaledLong());
	}

	@Test
	public void testNotCompact() {
		Decimal decimal1 = Decimal.fromBigDecimal(new BigDecimal(10), 20, 0);
		Decimal decimal2 = Decimal.fromBigDecimal(new BigDecimal(15), 20, 0);
		Assert.assertEquals(decimal1.hashCode(),
				Decimal.fromBigDecimal(new BigDecimal(10), 20, 0).hashCode());
		Assert.assertEquals(decimal1, decimal1.copy());
		Assert.assertEquals(decimal1, Decimal.fromBigDecimal(decimal1.toBigDecimal(), 20, 0));
		Assert.assertEquals(decimal1, Decimal.fromUnscaledBytes(20, 0, decimal1.toUnscaledBytes()));
		Assert.assertTrue(decimal1.compareTo(decimal2) < 0);
		Assert.assertEquals(1, decimal1.signum());
		Assert.assertTrue(10.5 == Decimal.castFrom(10.5, 20, 1).doubleValue());
		Assert.assertEquals(Decimal.fromBigDecimal(new BigDecimal(-10), 20, 0), decimal1.negate());
		Assert.assertEquals(decimal1, decimal1.abs());
		Assert.assertEquals(decimal1, decimal1.negate().abs());
		Assert.assertEquals(25, Decimal.add(decimal1, decimal2, 20, 0).toBigDecimal().longValue());
		Assert.assertEquals(-5, Decimal.subtract(decimal1, decimal2, 20, 0).toBigDecimal().longValue());
		Assert.assertEquals(150, Decimal.multiply(decimal1, decimal2, 20, 0).toBigDecimal().longValue());
		Assert.assertTrue(0.67 == Decimal.divide(decimal1, decimal2, 20, 2).doubleValue());
		Assert.assertEquals(decimal1, Decimal.mod(decimal1, decimal2, 20, 0));
		Assert.assertEquals(5, Decimal.divideToIntegralValue(
				decimal1, Decimal.fromBigDecimal(new BigDecimal(2), 20, 0), 20, 0).toBigDecimal().longValue());
		Assert.assertEquals(10, Decimal.castToIntegral(decimal1));
		Assert.assertEquals(true, Decimal.castToBoolean(decimal1));
		Assert.assertTrue(Decimal.compare(decimal1, 10) == 0);
		Assert.assertTrue(Decimal.compare(decimal1, 5) > 0);
		Assert.assertTrue(Decimal.compare(Decimal.fromBigDecimal(new BigDecimal(10.5), 20, 2), 10) > 0);
		Assert.assertEquals(Decimal.castFrom(1.0, 20, 5), Decimal.sign(Decimal.castFrom(5.556, 20, 5)));

		Assert.assertNull(Decimal.fromBigDecimal(new BigDecimal(Long.MAX_VALUE), 5, 0));
		Assert.assertEquals(0, Decimal.zero(20, 2).toBigDecimal().intValue());
		Assert.assertEquals(0, Decimal.zero(20, 2).toBigDecimal().intValue());
	}

	@Test
	public void testToString() {
		String val = "0.0000000000000000001";
		Assert.assertEquals(val, Decimal.castFrom(val, 39, val.length() - 2).toString());
		val = "123456789012345678901234567890123456789";
		Assert.assertEquals(val, Decimal.castFrom(val, 39, 0).toString());
	}
}
