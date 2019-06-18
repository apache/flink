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

package org.apache.flink.table.functions.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.ql.udf.UDFBase64;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link HiveSimpleUDF}.
 */
public class HiveSimpleUDFTest {
	@Test
	public void testUDFRand() {
		HiveSimpleUDF udf = init(UDFRand.class, new DataType[0]);

		double result = (double) udf.eval();

		assertTrue(result >= 0 && result < 1);
	}

	@Test
	public void testUDFBin() {
		HiveSimpleUDF udf = init(UDFBin.class, new DataType[]{ DataTypes.INT() });

		assertEquals("1100", udf.eval(12));
	}

	@Test
	public void testUDFConv() {
		HiveSimpleUDF udf = init(
			UDFConv.class,
			new DataType[]{
				DataTypes.STRING(),
				DataTypes.INT(),
				DataTypes.INT()
			});

		assertEquals("1", udf.eval("12", 2, 10));
		assertEquals("-16", udf.eval(-10, 16, -10));
	}

	@Test
	public void testUDFJson() {
		String pattern = "$.owner";
		String json = "{\"store\": \"test\", \"owner\": \"amy\"}";
		String expected = "amy";

		HiveSimpleUDF udf = init(
			UDFJson.class,
			new DataType[]{
				DataTypes.STRING(),
				DataTypes.STRING()
			});

		assertEquals(expected, udf.eval(json, pattern));

		udf = init(
			UDFJson.class,
			new DataType[]{
				DataTypes.CHAR(100),
				DataTypes.CHAR(pattern.length())
			});

		assertEquals(expected, udf.eval(json, pattern));

		udf = init(
			UDFJson.class,
			new DataType[]{
				DataTypes.VARCHAR(100),
				DataTypes.VARCHAR(pattern.length())
			});

		assertEquals(expected, udf.eval(json, pattern));

		// Test invalid CHAR length
		udf = init(
			UDFJson.class,
			new DataType[]{
				DataTypes.CHAR(100),
				DataTypes.CHAR(pattern.length() - 1) // shorter than pattern's length by 1
			});

		// Cannot find path "$.owne"
		assertEquals(null, udf.eval(json, pattern));
	}

	@Test
	public void testUDFMinute() {
		HiveSimpleUDF udf = init(
			UDFMinute.class,
			new DataType[]{
				DataTypes.STRING()
			});

		assertEquals(17, udf.eval("1969-07-20 20:17:40"));
		assertEquals(17, udf.eval(Timestamp.valueOf("1969-07-20 20:17:40")));
		assertEquals(58, udf.eval("12:58:59"));
	}

	@Test
	public void testUDFWeekOfYear() {
		HiveSimpleUDF udf = init(
			UDFWeekOfYear.class,
			new DataType[]{
				DataTypes.STRING()
			});

		assertEquals(29, udf.eval("1969-07-20"));
		assertEquals(29, udf.eval(Date.valueOf("1969-07-20")));
		assertEquals(29, udf.eval(Timestamp.valueOf("1969-07-20 00:00:00")));
		assertEquals(1, udf.eval("1980-12-31 12:59:59"));
	}

	@Test
	public void testUDFRegExpExtract() {
		HiveSimpleUDF udf = init(
			UDFRegExpExtract.class,
			new DataType[]{
				DataTypes.STRING(),
				DataTypes.STRING(),
				DataTypes.INT()
			});

		assertEquals("100", udf.eval("100-200", "(\\d+)-(\\d+)", 1));
	}

	@Test
	public void testUDFUnbase64() {
		HiveSimpleUDF udf = init(
			UDFBase64.class,
			new DataType[]{
				DataTypes.BYTES()
			});

		assertEquals("Cg==", udf.eval(new byte[] {10}));
	}

	@Test
	public void testUDFUnhex() throws UnsupportedEncodingException {
		HiveSimpleUDF udf = init(
			UDFUnhex.class,
			new DataType[]{
				DataTypes.STRING()
			});

		assertEquals("MySQL", new String((byte[]) udf.eval("4D7953514C"), "UTF-8"));
	}

	private HiveSimpleUDF init(Class hiveUdfClass, DataType[] argTypes) {
		HiveSimpleUDF udf = new HiveSimpleUDF(new HiveFunctionWrapper(hiveUdfClass.getName()));

		// Hive UDF won't have literal args
		udf.setArgumentTypesAndConstants(new Object[0], argTypes);
		udf.getHiveResultType(new Object[0], argTypes);

		udf.open(null);

		return udf;
	}
}
