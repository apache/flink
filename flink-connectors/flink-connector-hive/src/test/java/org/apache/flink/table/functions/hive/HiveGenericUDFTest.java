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
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.functions.hive.HiveSimpleUDFTest.HiveUDFCallContext;
import org.apache.flink.table.functions.hive.util.TestGenericUDFArray;
import org.apache.flink.table.functions.hive.util.TestGenericUDFStructSize;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCeil;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateDiff;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDecode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapKeys;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStringToMap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;

import static org.apache.flink.table.HiveVersionTestUtil.HIVE_110_OR_LATER;
import static org.apache.flink.table.HiveVersionTestUtil.HIVE_120_OR_LATER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link HiveGenericUDF}.
 */
public class HiveGenericUDFTest {
	private static HiveShim hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());

	@Test
	public void testAbs() {
		HiveGenericUDF udf = init(
			GenericUDFAbs.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.DOUBLE()
			}
		);

		assertEquals(10.0d, udf.eval(-10.0d));

		udf = init(
			GenericUDFAbs.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.INT()
			}
		);

		assertEquals(10, udf.eval(-10));

		udf = init(
			GenericUDFAbs.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.STRING()
			}
		);

		assertEquals(10.0, udf.eval("-10.0"));
	}

	@Test
	public void testAddMonths() throws Exception {
		Assume.assumeTrue(HIVE_110_OR_LATER);
		HiveGenericUDF udf = init(
			Class.forName("org.apache.hadoop.hive.ql.udf.generic.GenericUDFAddMonths"),
			new Object[] {
				null,
				1
			},
			new DataType[] {
				DataTypes.STRING(),
				DataTypes.INT()
			}
		);

		assertEquals("2009-09-30", udf.eval("2009-08-31", 1));
		assertEquals("2009-09-30", udf.eval("2009-08-31 11:11:11", 1));
	}

	@Test
	public void testDateFormat() throws Exception {
		Assume.assumeTrue(HIVE_120_OR_LATER);
		String constYear = "y";
		String constMonth = "M";

		HiveGenericUDF udf = init(
			Class.forName("org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateFormat"),
			new Object[] {
				null,
				constYear
			},
			new DataType[] {
				DataTypes.STRING(),
				DataTypes.STRING()
			}
		);

		assertEquals("2009", udf.eval("2009-08-31", constYear));

		udf = init(
			Class.forName("org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateFormat"),
			new Object[] {
				null,
				constMonth
			},
			new DataType[] {
				DataTypes.DATE(),
				DataTypes.STRING()
			}
		);

		assertEquals("8", udf.eval(Date.valueOf("2019-08-31"), constMonth));
	}

	@Test
	public void testDecode() {
		String constDecoding = "UTF-8";

		HiveGenericUDF udf = init(
			GenericUDFDecode.class,
			new Object[] {
				null,
				constDecoding
			},
			new DataType[] {
				DataTypes.BYTES(),
				DataTypes.STRING()
			}
		);

		HiveSimpleUDF simpleUDF = HiveSimpleUDFTest.init(
			UDFUnhex.class,
			new DataType[]{
				DataTypes.STRING()
			});

		assertEquals("MySQL", udf.eval(simpleUDF.eval("4D7953514C"), constDecoding));
	}

	@Test
	public void testCase() {
		HiveGenericUDF udf = init(
			GenericUDFCase.class,
			new Object[] {
				null,
				"1",
				"a",
				"b"
			},
			new DataType[] {
				DataTypes.STRING(),
				DataTypes.STRING(),
				DataTypes.STRING(),
				DataTypes.STRING()
			}
		);

		assertEquals("a", udf.eval("1", "1", "a", "b"));
		assertEquals("b", udf.eval("2", "1", "a", "b"));
	}

	@Test
	public void testCeil() {
		HiveGenericUDF udf = init(
			GenericUDFCeil.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.DOUBLE()
			}
		);

		assertEquals(0L, udf.eval(-0.1d));

		udf = init(
			GenericUDFCeil.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.DECIMAL(2, 1)
			}
		);

		assertEquals(BigDecimal.valueOf(4), udf.eval(BigDecimal.valueOf(3.1d)));
	}

	@Test
	public void testCoalesce() {
		HiveGenericUDF udf = init(
			GenericUDFCoalesce.class,
			new Object[] {
				null,
				1,
				null
			},
			new DataType[] {
				DataTypes.INT(),
				DataTypes.INT(),
				DataTypes.INT()
			}
		);

		assertEquals(1, udf.eval(null, 1, null));
	}

	@Test
	public void testDateDiff() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		String d = "1969-07-20";
		String t1 = "1969-07-20 00:00:00";
		String t2 = "1980-12-31 12:59:59";

		HiveGenericUDF udf = init(
			GenericUDFDateDiff.class,
			new Object[] {
				null,
				null
			},
			new DataType[] {
				DataTypes.VARCHAR(20),
				DataTypes.CHAR(20),
			}
		);

		assertEquals(-4182, udf.eval(t1, t2));

		udf = init(
			GenericUDFDateDiff.class,
			new Object[] {
				null,
				null
			},
			new DataType[] {
				DataTypes.DATE(),
				DataTypes.TIMESTAMP(),
			}
		);

		assertEquals(-4182, udf.eval(Date.valueOf(d), Timestamp.valueOf(t2)));

		// Test invalid char length
		udf = init(
			GenericUDFDateDiff.class,
			new Object[] {
				null,
				null
			},
			new DataType[] {
				DataTypes.CHAR(2),
				DataTypes.VARCHAR(2),
			}
		);

		assertEquals(null, udf.eval(t1, t2));
	}

	@Test
	public void testArray() {
		HiveGenericUDF udf = init(
			TestGenericUDFArray.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.ARRAY(DataTypes.INT())
			}
		);

		assertEquals(6, udf.eval(1, 2, 3));
		assertEquals(6, udf.eval(new Integer[] { 1, 2, 3 }));
	}

	@Test
	public void testMap() {
		// test output as map
		String testInput = "1:1,2:2,3:3";

		HiveGenericUDF udf = init(
			GenericUDFStringToMap.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.VARCHAR(testInput.length())
			}
		);

		assertEquals(
			new HashMap<String, String>() {{
				put("1", "1");
				put("2", "2");
				put("3", "3");
			}},
			udf.eval(testInput));

		// test input as map and nested functions
		HiveGenericUDF udf2 = init(
			GenericUDFMapKeys.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
			}
		);

		Object[] result = (Object[]) udf2.eval(udf.eval(testInput));

		assertEquals(3, result.length);
		assertThat(Arrays.asList(result), containsInAnyOrder("1", "2", "3"));
	}

	@Test
	public void testStruct() {
		HiveGenericUDF udf = init(
			GenericUDFStruct.class,
			new Object[] {
				null,
				null,
				null
			},
			new DataType[] {
				DataTypes.INT(),
				DataTypes.CHAR(2),
				DataTypes.VARCHAR(10)
			}
		);

		Row result = (Row) udf.eval(1, "222", "3");

		assertEquals(Row.of(1, "22", "3"), result);

		udf = init(
			TestGenericUDFStructSize.class,
			new Object[] {
				null
			},
			new DataType[] {
				DataTypes.ROW(
					DataTypes.FIELD("1", DataTypes.INT()),
					DataTypes.FIELD("2", DataTypes.CHAR(2)),
					DataTypes.FIELD("3", DataTypes.VARCHAR(10))
				)
			}
		);

		assertEquals(3, udf.eval(result));
	}

	private static HiveGenericUDF init(Class hiveUdfClass, Object[] constantArgs, DataType[] argTypes) {
		HiveGenericUDF udf = new HiveGenericUDF(new HiveFunctionWrapper(hiveUdfClass.getName()), hiveShim);

		CallContext callContext = new HiveUDFCallContext(constantArgs, argTypes);
		udf.getTypeInference(null).getOutputTypeStrategy().inferType(callContext);

		udf.open(null);

		return udf;
	}
}
