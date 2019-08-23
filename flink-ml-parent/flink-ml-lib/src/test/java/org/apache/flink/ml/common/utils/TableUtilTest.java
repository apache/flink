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

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit test for TableUtil.
 */
public class TableUtilTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	private String[] colNames = new String[] {"f0", "f1", "f2"};
	private TableSchema tableSchema = new TableSchema(colNames,
		new TypeInformation[] {Types.INT, Types.LONG, Types.STRING});

	@Test
	public void testFindIndexFromName() {
		String[] colNames = new String[] {"f0", "f1", "F2"};
		Assert.assertEquals(TableUtil.findColIndex(colNames, "f0"), 0);
		Assert.assertEquals(TableUtil.findColIndex(colNames, "F1"), 1);
		Assert.assertEquals(TableUtil.findColIndex(colNames, "f3"), -1);
		Assert.assertEquals(TableUtil.findColIndex(tableSchema, "f0"), 0);
		Assert.assertArrayEquals(TableUtil.findColIndices(colNames, new String[] {"f1", "F2"}), new int[] {1, 2});
		Assert.assertArrayEquals(TableUtil.findColIndices(tableSchema, new String[] {"f1", "F2"}), new int[] {1, 2});
	}

	@Test
	public void testFindTypeFromTable() {
		Assert.assertArrayEquals(TableUtil.findColTypes(tableSchema, new String[] {"f0", "f1"}),
			new TypeInformation[] {TypeInformation.of(Integer.class), Types.LONG});

		Assert.assertArrayEquals(TableUtil.findColTypes(tableSchema, new String[] {"f1", "f3"}),
			new TypeInformation[] {Types.LONG, null});

		Assert.assertEquals(TableUtil.findColType(tableSchema, "f0"), TypeInformation.of(Integer.class));
		Assert.assertNull(TableUtil.findColType(tableSchema, "f3"));
	}

	@Test
	public void isNumberIsStringTest() {
		Assert.assertTrue(TableUtil.isNumber(Types.INT));
		Assert.assertTrue(TableUtil.isNumber(Types.DOUBLE));
		Assert.assertTrue(TableUtil.isNumber(Types.LONG));
		Assert.assertTrue(TableUtil.isNumber(Types.BYTE));
		Assert.assertTrue(TableUtil.isNumber(Types.FLOAT));
		Assert.assertTrue(TableUtil.isNumber(Types.SHORT));
		Assert.assertFalse(TableUtil.isNumber(Types.STRING));
		Assert.assertTrue(TableUtil.isString(Types.STRING));
	}

	@Test
	public void assertTest() {
		String[] colNames = new String[] {"f0", "f1", "f2"};
		TableUtil.assertSelectedColExist(colNames, "f0");

		thrown.expect(RuntimeException.class);
		TableUtil.assertSelectedColExist(colNames, "f3");

		thrown.expect(RuntimeException.class);
		TableUtil.assertSelectedColExist(colNames, "f0", "f3");

		TableUtil.assertNumericalCols(tableSchema, "f0", "f1");

		thrown.expect(RuntimeException.class);
		TableUtil.assertNumericalCols(tableSchema, "f0", "f2");

		TableUtil.assertStringCols(tableSchema, "f3");

		thrown.expect(RuntimeException.class);
		TableUtil.assertStringCols(tableSchema, "f2");

		thrown.expect(RuntimeException.class);
		TableUtil.assertStringCols(tableSchema, "f0", "f3");
	}

	@Test
	public void getNumericColsTest() {
		TableSchema tableSchema = new TableSchema(new String[] {"f0", "f1", "F2", "f3"},
			new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});

		Assert.assertArrayEquals(TableUtil.getNumericCols(tableSchema), new String[] {"f0", "f1"});
		Assert.assertArrayEquals(TableUtil.getNumericCols(tableSchema, new String[] {"f0"}), new String[] {"f1"});
		Assert.assertArrayEquals(TableUtil.getNumericCols(tableSchema, new String[] {"f2"}), new String[] {"f0", "f1"});
	}

	@Test
	public void getCategoricalColsTest() {
		TableSchema tableSchema = new TableSchema(new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation[] {Types.INT, Types.LONG, Types.STRING, Types.BOOLEAN});

		Assert.assertArrayEquals(TableUtil.getCategoricalCols(tableSchema, tableSchema.getFieldNames(), null),
			new String[] {"f2", "f3"});
		Assert.assertArrayEquals(
			TableUtil.getCategoricalCols(tableSchema, new String[]{"f2", "f1", "f0", "f3"}, new String[] {"f0"}),
			new String[] {"f2", "f0", "f3"});

		thrown.expect(IllegalArgumentException.class);
		Assert.assertArrayEquals(
			TableUtil.getCategoricalCols(tableSchema, new String[] {"f3", "f0"}, new String[] {"f2"}),
			new String[] {"f3", "f2"});
	}
}
