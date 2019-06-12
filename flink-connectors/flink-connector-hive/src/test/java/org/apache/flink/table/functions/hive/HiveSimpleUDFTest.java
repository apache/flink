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

import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.junit.Test;

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
	public void testUDFJson() {
		HiveSimpleUDF udf = init(
			UDFJson.class,
			new DataType[]{
				DataTypes.STRING(),
				DataTypes.STRING()
			});

		assertEquals("amy", udf.eval("{\"store\": \"test\", \"owner\": \"amy\"}", "$.owner"));
	}

	@Test
	public void testUDFMinute() {
		HiveSimpleUDF udf = init(
			UDFMinute.class,
			new DataType[]{
				DataTypes.STRING()
			});

		assertEquals(17, udf.eval("1969-07-20 20:17:40"));
	}

	@Test
	public void testUDFWeekOfYear() {
		HiveSimpleUDF udf = init(
			UDFWeekOfYear.class,
			new DataType[]{
				DataTypes.STRING()
			});

		assertEquals(29, udf.eval("1969-07-20"));
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
