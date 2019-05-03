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

package org.apache.flink.table.types;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DataType}, its subclasses and {@link DataTypes}.
 */
public class DataTypesTest {

	@Test
	public void testDataType() {
		final DataType dataType = DataTypes.TIMESTAMP(9).bridgedTo(java.sql.Timestamp.class).notNull();

		assertEquals(java.sql.Timestamp.class, dataType.getConversionClass());
		testLogicalType(new TimestampType(false, 9), dataType);

		testLogicalType(new TimestampType(true, 9), dataType.andNull());

		try {
			DataTypes.TIMESTAMP(3).bridgedTo(DataTypesTest.class);
			fail("Invalid conversion class expected.");
		} catch (ValidationException e) {
			// ok
		}
	}

	@Test
	public void testAtomicDataTypes() {
		testLogicalType(new CharType(2), DataTypes.CHAR(2));
		testLogicalType(new VarCharType(2), DataTypes.VARCHAR(2));
		testLogicalType(new VarCharType(Integer.MAX_VALUE), DataTypes.STRING());

		testLogicalType(
			new DayTimeIntervalType(DayTimeResolution.MINUTE_TO_SECOND, DayTimeIntervalType.DEFAULT_DAY_PRECISION, 2),
			DataTypes.INTERVAL(DataTypes.MINUTE(), DataTypes.SECOND(2)));

		testLogicalType(
			new YearMonthIntervalType(YearMonthResolution.MONTH),
			DataTypes.INTERVAL(DataTypes.MONTH()));

		try {
			DataTypes.INTERVAL(DataTypes.MONTH(), DataTypes.YEAR(2));
			fail("Invalid interval expected.");
		} catch (ValidationException e) {
			// ok
		}
	}

	@Test
	public void testElementDataTypes() {
		testLogicalType(
			new ArrayType(new ArrayType(new IntType())),
			DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())));

		testLogicalType(
			new MultisetType(new MultisetType(false, new IntType())),
			DataTypes.MULTISET(DataTypes.MULTISET(DataTypes.INT()).notNull()));
	}

	@Test
	public void testKeyValueDataTypes() {
		testLogicalType(
			new MapType(new IntType(), new SmallIntType()),
			DataTypes.MAP(DataTypes.INT(), DataTypes.SMALLINT()));
	}

	@Test
	public void testFieldsDataTypes() {
		final DataType.FieldsDataType dataType = DataTypes.ROW(
			DataTypes.FIELD("field1", DataTypes.CHAR(2)),
			DataTypes.FIELD("field2", DataTypes.BOOLEAN()));
		testLogicalType(
			new RowType(
				Arrays.asList(
				new RowType.RowField("field1", new CharType(2)),
				new RowType.RowField("field2", new BooleanType()))),
			dataType);

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("field1", DataTypes.CHAR(2));
		fields.put("field2", DataTypes.BOOLEAN());
		assertEquals(fields, dataType.getFieldDataTypes());
	}

	// --------------------------------------------------------------------------------------------

	private static void testLogicalType(LogicalType logicalType, DataType dataType) {
		assertEquals(logicalType, dataType.getLogicalType());
	}
}
