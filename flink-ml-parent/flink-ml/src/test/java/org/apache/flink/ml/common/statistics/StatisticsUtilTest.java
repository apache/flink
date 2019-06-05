/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.utils.Types;
import org.apache.flink.table.api.TableSchema;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for StatistictUtil.
 */
public class StatisticsUtilTest {

	@Test
	public void isString() {
		assertFalse(StatisticsUtil.isString("Double"));
		assertTrue(StatisticsUtil.isString("String"));
	}

	@Test
	public void isBoolean() {
		assertFalse(StatisticsUtil.isBoolean("String"));
		assertTrue(StatisticsUtil.isBoolean("Boolean"));
	}

	@Test
	public void isDatetime() {
		assertFalse(StatisticsUtil.isDatetime("String"));
		assertTrue(StatisticsUtil.isDatetime("datetime"));
	}

	@Test
	public void toTypeString() {
		assertEquals("double", StatisticsUtil.toTypeString(Types.DOUBLE));
		assertEquals("string", StatisticsUtil.toTypeString(Types.STRING));
	}

	@Test
	public void isNumber() {
		assertFalse(StatisticsUtil.isNumber("String"));
		assertFalse(StatisticsUtil.isNumber("Long"));
	}

	@Test
	public void isNumber2() {
		assertFalse(StatisticsUtil.isNumber(Types.STRING));
		assertTrue(StatisticsUtil.isNumber(Types.LONG));
	}

	@Test
	public void isNumber3() {
		TableSchema schema = new TableSchema(new String[] {"f0", "f1"},
			new TypeInformation[] {Types.STRING, Types.DOUBLE});
		StatisticsUtil.isNumber(schema, new String[] {"f1"});
	}

	@Test
	public void testRound() {
		double t = 3.2499999;
		assertEquals(3.25, StatisticsUtil.round(t, 2), 10e-10);

	}
}
