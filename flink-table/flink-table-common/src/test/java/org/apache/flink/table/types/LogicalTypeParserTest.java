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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.UNRESOLVED;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LogicalTypeParser}.
 */
@RunWith(Parameterized.class)
public class LogicalTypeParserTest {

	@Parameters(name = "{index}: [From: {0}, To: {1}]")
	public static List<Object[]> testData() {
		return Arrays.asList(
			new Object[][]{

				{"CHAR", new CharType(), null},

				{"CHAR NOT NULL", new CharType().copy(false), null},

				{"CHAR   NOT \t\nNULL", new CharType().copy(false), null},

				{"char not null", new CharType().copy(false), null},

				{"CHAR NULL", new CharType(), null},

				{"CHAR(33)", new CharType(33), null},

				{"VARCHAR", new VarCharType(), null},

				{"VARCHAR(33)", new VarCharType(33), null},

				{"STRING", new VarCharType(VarCharType.MAX_LENGTH), null},

				{"BOOLEAN", new BooleanType(), null},

				{"BINARY", new BinaryType(), null},

				{"BINARY(33)", new BinaryType(33), null},

				{"VARBINARY", new VarBinaryType(), null},

				{"VARBINARY(33)", new VarBinaryType(33), null},

				{"BYTES", new VarBinaryType(VarBinaryType.MAX_LENGTH), null},

				{"DECIMAL", new DecimalType(), null},

				{"DEC", new DecimalType(), null},

				{"NUMERIC", new DecimalType(), null},

				{"DECIMAL(10)", new DecimalType(10), null},

				{"DEC(10)", new DecimalType(10), null},

				{"NUMERIC(10)", new DecimalType(10), null},

				{"DECIMAL(10, 3)", new DecimalType(10, 3), null},

				{"DEC(10, 3)", new DecimalType(10, 3), null},

				{"NUMERIC(10, 3)", new DecimalType(10, 3), null},

				{"TINYINT", new TinyIntType(), null},

				{"SMALLINT", new SmallIntType(), null},

				{"INTEGER", new IntType(), null},

				{"INT", new IntType(), null},

				{"BIGINT", new BigIntType(), null},

				{"FLOAT", new FloatType(), null},

				{"DOUBLE", new DoubleType(), null},

				{"DOUBLE PRECISION", new DoubleType(), null},

				{"DATE", new DateType(), null},

				{"TIME", new TimeType(), null},

				{"TIME(3)", new TimeType(3), null},

				{"TIME WITHOUT TIME ZONE", new TimeType(), null},

				{"TIME(3) WITHOUT TIME ZONE", new TimeType(3), null},

				{"TIMESTAMP", new TimestampType(), null},

				{"TIMESTAMP(3)", new TimestampType(3), null},

				{"TIMESTAMP WITHOUT TIME ZONE", new TimestampType(), null},

				{"TIMESTAMP(3) WITHOUT TIME ZONE", new TimestampType(3), null},

				{"TIMESTAMP WITH TIME ZONE", new ZonedTimestampType(), null},

				{"TIMESTAMP(3) WITH TIME ZONE", new ZonedTimestampType(3), null},

				{"TIMESTAMP WITH LOCAL TIME ZONE", new LocalZonedTimestampType(), null},

				{"TIMESTAMP(3) WITH LOCAL TIME ZONE", new LocalZonedTimestampType(3), null},

				{
					"INTERVAL YEAR",
					new YearMonthIntervalType(YearMonthResolution.YEAR),
					null
				},

				{
					"INTERVAL YEAR(4)",
					new YearMonthIntervalType(YearMonthResolution.YEAR, 4),
					null
				},

				{
					"INTERVAL MONTH",
					new YearMonthIntervalType(YearMonthResolution.MONTH),
					null
				},

				{
					"INTERVAL YEAR TO MONTH",
					new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH),
					null
				},

				{
					"INTERVAL YEAR(4) TO MONTH",
					new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH, 4),
					null
				},

				{
					"INTERVAL DAY(2) TO SECOND(3)",
					new DayTimeIntervalType(DayTimeResolution.DAY_TO_SECOND, 2, 3),
					null
				},

				{
					"INTERVAL HOUR TO SECOND(3)",
					new DayTimeIntervalType(
						DayTimeResolution.HOUR_TO_SECOND,
						DayTimeIntervalType.DEFAULT_DAY_PRECISION,
						3),
					null
				},

				{
					"INTERVAL MINUTE",
					new DayTimeIntervalType(DayTimeResolution.MINUTE),
					null
				},

				{"ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>", new ArrayType(new LocalZonedTimestampType(3)), null},

				{"ARRAY<INT NOT NULL>", new ArrayType(new IntType(false)), null},

				{"INT ARRAY", new ArrayType(new IntType()), null},

				{"INT NOT NULL ARRAY", new ArrayType(new IntType(false)), null},

				{"INT ARRAY NOT NULL", new ArrayType(false, new IntType()), null},

				{"MULTISET<INT NOT NULL>", new MultisetType(new IntType(false)), null},

				{"INT MULTISET", new MultisetType(new IntType()), null},

				{"INT NOT NULL MULTISET", new MultisetType(new IntType(false)), null},

				{"INT MULTISET NOT NULL", new MultisetType(false, new IntType()), null},

				{"MAP<BIGINT, BOOLEAN>", new MapType(new BigIntType(), new BooleanType()), null},

				{
					"ROW<f0 INT NOT NULL, f1 BOOLEAN>",
					new RowType(
						Arrays.asList(
							new RowType.RowField("f0", new IntType(false)),
							new RowType.RowField("f1", new BooleanType()))),
					null
				},

				{
					"ROW(f0 INT NOT NULL, f1 BOOLEAN)",
					new RowType(
						Arrays.asList(
							new RowType.RowField("f0", new IntType(false)),
							new RowType.RowField("f1", new BooleanType()))),
					null
				},

				{
					"ROW<`f0` INT>",
					new RowType(
						Collections.singletonList(new RowType.RowField("f0", new IntType()))),
					null
				},

				{
					"ROW(`f0` INT)",
					new RowType(
						Collections.singletonList(new RowType.RowField("f0", new IntType()))),
					null
				},

				{
					"ROW<>",
					new RowType(Collections.emptyList()),
					null
				},

				{
					"ROW()",
					new RowType(Collections.emptyList()),
					null
				},

				{
					"ROW<f0 INT NOT NULL 'This is a comment.', f1 BOOLEAN 'This as well.'>",
					new RowType(
						Arrays.asList(
							new RowType.RowField("f0", new IntType(false), "This is a comment."),
							new RowType.RowField("f1", new BooleanType(), "This as well."))),
					null
				},

				{"NULL", new NullType(), null},

				{
					createAnyType(LogicalTypeParserTest.class).asSerializableString(),
					createAnyType(LogicalTypeParserTest.class),
					null
				},

				{"cat.db.MyType", new UnresolvedUserDefinedType("cat", "db", "MyType"), null},

				{"`db`.`MyType`", new UnresolvedUserDefinedType(null, "db", "MyType"), null},

				{"MyType", new UnresolvedUserDefinedType(null, null, "MyType"), null},

				{"ARRAY<MyType>", new ArrayType(new UnresolvedUserDefinedType(null, null, "MyType")), null},

				{
					"ROW<f0 MyType, f1 `c`.`d`.`t`>",
					RowType.of(
						new UnresolvedUserDefinedType(null, null, "MyType"),
						new UnresolvedUserDefinedType("c", "d", "t")),
					null
				},

				// error message testing

				{"ROW<`f0", null, "Unexpected end"},

				{"ROW<`f0`", null, "Unexpected end"},

				{"VARCHAR(test)", null, "<LITERAL_INT> expected"},

				{"VARCHAR(33333333333)", null, "Invalid integer value"},

				{"ROW<field INT, field2>", null, "<KEYWORD> expected"},

				{"ANY('unknown.class', '')", null, "Unable to restore the ANY type"}
			}
		);
	}

	@Parameter
	public String typeString;

	@Parameter(1)
	public LogicalType type;

	@Parameter(2)
	public String errorMessage;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testParsing() {
		if (errorMessage == null) {
			assertThat(
				LogicalTypeParser.parse(typeString),
				equalTo(type));
		}
	}

	@Test
	public void testSerializableParsing() {
		if (errorMessage == null) {
			if (!hasRoot(type, UNRESOLVED) &&
					type.getChildren().stream().noneMatch(t -> hasRoot(t, UNRESOLVED))) {
				assertThat(
					LogicalTypeParser.parse(type.asSerializableString()),
					equalTo(type));
			}
		}
	}

	@Test
	public void testErrorMessage() {
		if (errorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectMessage(errorMessage);

			LogicalTypeParser.parse(typeString);
		}
	}

	// --------------------------------------------------------------------------------------------

	private static <T> AnyType<T> createAnyType(Class<T> clazz) {
		return new AnyType<>(clazz, new KryoSerializer<>(clazz, new ExecutionConfig()));
	}
}
