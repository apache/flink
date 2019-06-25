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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LogicalTypeCasts}.
 */
@RunWith(Parameterized.class)
public class LogicalTypeCastsTest {

	@Parameters(name = "{index}: [From: {0}, To: {1}, Implicit: {2}, Explicit: {3}]")
	public static List<Object[]> testData() {
		return Arrays.asList(
			new Object[][]{
				{DataTypes.SMALLINT(), DataTypes.BIGINT(), true, true},

				// nullability does not match
				{DataTypes.SMALLINT().notNull(), DataTypes.SMALLINT(), true, true},

				{DataTypes.SMALLINT(), DataTypes.SMALLINT().notNull(), false, false},

				{DataTypes.INTERVAL(DataTypes.YEAR()), DataTypes.SMALLINT(), true, true},

				// not an interval with single field
				{DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()), DataTypes.SMALLINT(), false, false},

				{DataTypes.INT(), DataTypes.DECIMAL(5, 5), true, true},

				// loss of precision
				{DataTypes.FLOAT(), DataTypes.INT(), false, true},

				{DataTypes.STRING(), DataTypes.FLOAT(), false, true},

				{DataTypes.FLOAT(), DataTypes.STRING(), false, true},

				{DataTypes.DECIMAL(3, 2), DataTypes.STRING(), false, true},

				{
					DataTypes.ANY(Types.GENERIC(LogicalTypesTest.class)),
					DataTypes.ANY(Types.GENERIC(LogicalTypesTest.class)),
					true,
					true
				},

				{
					DataTypes.ANY(Types.GENERIC(LogicalTypesTest.class)),
					DataTypes.ANY(Types.GENERIC(Object.class)),
					false,
					false
				},

				{DataTypes.NULL(), DataTypes.INT(), true, true},

				{DataTypes.ARRAY(DataTypes.INT()), DataTypes.ARRAY(DataTypes.BIGINT()), true, true},

				{DataTypes.ARRAY(DataTypes.INT()), DataTypes.ARRAY(DataTypes.STRING()), false, true},

				{
					DataTypes.ROW(
						DataTypes.FIELD("f1", DataTypes.INT()),
						DataTypes.FIELD("f2", DataTypes.INT())),
					DataTypes.ROW(
						DataTypes.FIELD("f1", DataTypes.INT()),
						DataTypes.FIELD("f2", DataTypes.BIGINT())),
					true,
					true
				},

				{
					DataTypes.ROW(
						DataTypes.FIELD("f1", DataTypes.INT(), "description"),
						DataTypes.FIELD("f2", DataTypes.INT())),
					DataTypes.ROW(
						DataTypes.FIELD("f1", DataTypes.INT()),
						DataTypes.FIELD("f2", DataTypes.BIGINT())),
					true,
					true
				},

				{
					DataTypes.ROW(
						DataTypes.FIELD("f1", DataTypes.INT()),
						DataTypes.FIELD("f2", DataTypes.INT())),
					DataTypes.ROW(
						DataTypes.FIELD("f1", DataTypes.INT()),
						DataTypes.FIELD("f2", DataTypes.BOOLEAN())),
					false,
					false
				},

				{
					DataTypes.ROW(
						DataTypes.FIELD("f1", DataTypes.INT()),
						DataTypes.FIELD("f2", DataTypes.INT())),
					DataTypes.STRING(),
					false,
					false
				}
			}
		);
	}

	@Parameter
	public DataType sourceType;

	@Parameter(1)
	public DataType targetType;

	@Parameter(2)
	public boolean supportsImplicit;

	@Parameter(3)
	public boolean supportsExplicit;

	@Test
	public void testImplicitCasting() {
		assertThat(
			LogicalTypeCasts.supportsImplicitCast(sourceType.getLogicalType(), targetType.getLogicalType()),
			equalTo(supportsImplicit));
	}

	@Test
	public void testExplicitCasting() {
		assertThat(
			LogicalTypeCasts.supportsExplicitCast(sourceType.getLogicalType(), targetType.getLogicalType()),
			equalTo(supportsExplicit));
	}
}
