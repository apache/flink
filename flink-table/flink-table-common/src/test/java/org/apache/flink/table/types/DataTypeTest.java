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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.YEAR;
import static org.apache.flink.table.types.TypeTestingUtils.hasConversionClass;
import static org.apache.flink.table.types.TypeTestingUtils.hasNullability;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link DataType}.
 */
public class DataTypeTest {

	@Test
	public void testNullability() {
		assertThat(
			BIGINT().nullable(),
			hasNullability(true));

		assertThat(
			BIGINT().notNull(),
			hasNullability(false));

		assertThat(
			BIGINT().notNull().nullable(),
			hasNullability(true));
	}

	@Test
	public void testAtomicConversion() {
		assertThat(
			TIMESTAMP(0).bridgedTo(java.sql.Timestamp.class),
			hasConversionClass(java.sql.Timestamp.class));
	}

	@Test
	public void testTolerantAtomicConversion() {
		// this is logically only supported as input type because of
		// nullability but is tolerated until the planner complains
		// about an output type
		assertThat(
			BIGINT().nullable().bridgedTo(long.class),
			hasConversionClass(long.class));
	}

	@Test(expected = ValidationException.class)
	public void testInvalidAtomicConversion() {
		TIMESTAMP(0).bridgedTo(DataTypesTest.class);
	}

	@Test
	public void testArrayElementConversion() {
		assertThat(
			ARRAY(ARRAY(INT().notNull().bridgedTo(int.class))),
			hasConversionClass(int[][].class));
	}

	@Test
	public void testTolerantArrayConversion() {
		// this is logically only supported as input type because of
		// nullability but is tolerated until the planner complains
		// about an output type
		assertThat(
			ARRAY(ARRAY(INT().nullable())).bridgedTo(int[][].class),
			hasConversionClass(int[][].class));
	}

	@Test(expected = ValidationException.class)
	public void testInvalidArrayConversion() {
		ARRAY(ARRAY(INT())).bridgedTo(int[][][].class);
	}

	@Test
	public void testTolerantMapConversion() {
		// this doesn't make much sense logically but is supported until the planner complains
		assertThat(
			MULTISET(MULTISET(INT().bridgedTo(int.class))),
			hasConversionClass(Map.class));
	}

	@Test
	public void testFields() {
		final DataType rowDataType = ROW(FIELD("field1", CHAR(2)), FIELD("field2", BOOLEAN()));

		final List<DataType> fields = Arrays.asList(
			CHAR(2),
			BOOLEAN()
		);
		assertEquals(fields, rowDataType.getChildren());
	}

	@Test(expected = ValidationException.class)
	public void testInvalidOrderInterval() {
		INTERVAL(MONTH(), YEAR(2));
	}

	@Test
	public void testConversionEquality() {
		assertEquals(DataTypes.VARCHAR(2).bridgedTo(String.class), DataTypes.VARCHAR(2));
	}

	@Test
	public void testArrayInternalElementConversion() {
		final DataType arrayDataType = ARRAY(STRING()).bridgedTo(ArrayData.class);

		final List<DataType> children = Collections.singletonList(
			STRING().bridgedTo(StringData.class));

		assertEquals(children, arrayDataType.getChildren());
	}

	@Test
	public void testMapInternalElementConversion() {
		final DataType mapDataType = MAP(STRING(), ROW()).bridgedTo(MapData.class);

		final List<DataType> children = Arrays.asList(
			STRING().bridgedTo(StringData.class),
			ROW().bridgedTo(RowData.class));

		assertEquals(children, mapDataType.getChildren());
	}
}
