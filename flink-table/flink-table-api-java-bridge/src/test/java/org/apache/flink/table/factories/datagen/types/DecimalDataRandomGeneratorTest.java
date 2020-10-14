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

package org.apache.flink.table.factories.datagen.types;

import org.apache.flink.table.data.DecimalData;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests that the data generator is valid for every combination of precision and scale.
 */
public class DecimalDataRandomGeneratorTest {

	@Test
	public void testGenerateDecimalValues() {
		for (int precision = 1; precision <= 38; precision++) {
			for (int scale = 0; scale <= precision; scale++) {
				DecimalDataRandomGenerator gen = new DecimalDataRandomGenerator(precision, scale, Double.MIN_VALUE, Double.MAX_VALUE);

				DecimalData value = gen.next();
				Assert.assertNotNull("Null value for DECIMAL(" + precision + "," + scale + ")", value);

				String strRepr = String.valueOf(value);
				if (strRepr.charAt(0) == '-') {
					// drop the negative sign
					strRepr = strRepr.substring(1);
				}

				if (scale != precision) {
					// need to account for decimal . and potential leading zeros
					Assert.assertThat("Wrong length for DECIMAL(" + precision + "," + scale + ") = " + strRepr, strRepr.length(), lessThanOrEqualTo(precision + 1));
				} else {
					// need to account for decimal . and potential leading zeros
					Assert.assertThat("Wrong length for DECIMAL(" + precision + "," + scale + ") = " + strRepr, strRepr.length(), lessThanOrEqualTo(precision + 2));
				}
				if (scale != 0) {
					String decimalPart = strRepr.split("\\.")[1];
					Assert.assertThat("Wrong length for DECIMAL(" + precision + "," + scale + ") = " + strRepr, decimalPart.length(), equalTo(scale));
				}
			}
		}
	}

	@Test
	public void testMinMax() {
		for (int precision = 1; precision <= 38; precision++) {
			for (int scale = 0; scale <= precision; scale++) {
				BigDecimal min = BigDecimal.valueOf(-10.0);
				BigDecimal max = BigDecimal.valueOf(10.0);

				DecimalDataRandomGenerator gen = new DecimalDataRandomGenerator(precision, scale, min.doubleValue(), max.doubleValue());
				DecimalData result = gen.next();

				Assert.assertNotNull("Null value for DECIMAL(" + precision + "," + scale + ")", result);
				Assert.assertThat("value must be greater than or equal to min", result.toBigDecimal(), greaterThanOrEqualTo(min));
				Assert.assertThat("value must be less than or equal to max", result.toBigDecimal(), lessThanOrEqualTo(max));
			}
		}
	}
}
