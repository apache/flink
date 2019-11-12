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

import javax.annotation.Nullable;

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
	public static List<TestSpec> testData() {
		return Arrays.asList(

			TestSpec
				.forString("CHAR")
				.expectType(new CharType()),

			TestSpec
				.forString("CHAR NOT NULL")
				.expectType(new  CharType().copy(false)),

			TestSpec
				.forString("CHAR   NOT \t\nNULL")
				.expectType(new  CharType().copy(false)),

			TestSpec
				.forString("char not null")
				.expectType(new CharType().copy(false)),

			TestSpec
				.forString("CHAR NULL")
				.expectType(new CharType()),

			TestSpec
				.forString("CHAR(33)")
				.expectType(new CharType(33)),

			TestSpec
				.forString("VARCHAR")
				.expectType(new VarCharType()),

			TestSpec
				.forString("VARCHAR(33)")
				.expectType(new VarCharType(33)),

			TestSpec
				.forString("STRING")
				.expectType(new VarCharType(VarCharType.MAX_LENGTH)),

			TestSpec
				.forString("BOOLEAN")
				.expectType(new BooleanType()),

			TestSpec
				.forString("BINARY")
				.expectType(new BinaryType()),

			TestSpec
				.forString("BINARY(33)")
				.expectType(new BinaryType(33)),

			TestSpec
				.forString("VARBINARY")
				.expectType(new VarBinaryType()),

			TestSpec
				.forString("VARBINARY(33)")
				.expectType(new VarBinaryType(33)),

			TestSpec
				.forString("BYTES")
				.expectType(new VarBinaryType(VarBinaryType.MAX_LENGTH)),

			TestSpec
				.forString("DECIMAL")
				.expectType(new DecimalType()),

			TestSpec
				.forString("DEC")
				.expectType(new DecimalType()),

			TestSpec
				.forString("NUMERIC")
				.expectType(new DecimalType()),

			TestSpec
				.forString("DECIMAL(10)")
				.expectType(new DecimalType(10)),

			TestSpec
				.forString("DEC(10)")
				.expectType(new DecimalType(10)),

			TestSpec
				.forString("NUMERIC(10)")
				.expectType(new DecimalType(10)),

			TestSpec
				.forString("DECIMAL(10, 3)")
				.expectType(new DecimalType(10, 3)),

			TestSpec
				.forString("DEC(10, 3)")
				.expectType(new DecimalType(10, 3)),

			TestSpec
				.forString("NUMERIC(10, 3)")
				.expectType(new DecimalType(10, 3)),

			TestSpec
				.forString("TINYINT")
				.expectType(new TinyIntType()),

			TestSpec
				.forString("SMALLINT")
				.expectType(new SmallIntType()),

			TestSpec
				.forString("INTEGER")
				.expectType(new IntType()),

			TestSpec
				.forString("INT")
				.expectType(new IntType()),

			TestSpec
				.forString("BIGINT")
				.expectType(new BigIntType()),

			TestSpec
				.forString("FLOAT")
				.expectType(new FloatType()),

			TestSpec
				.forString("DOUBLE")
				.expectType(new DoubleType()),

			TestSpec
				.forString("DOUBLE PRECISION")
				.expectType(new DoubleType()),

			TestSpec
				.forString("DATE")
				.expectType(new DateType()),

			TestSpec
				.forString("TIME")
				.expectType(new TimeType()),

			TestSpec
				.forString("TIME(3)")
				.expectType(new TimeType(3)),

			TestSpec
				.forString("TIME WITHOUT TIME ZONE")
				.expectType(new TimeType()),

			TestSpec
				.forString("TIME(3) WITHOUT TIME ZONE")
				.expectType(new TimeType(3)),

			TestSpec
				.forString("TIMESTAMP")
				.expectType(new TimestampType()),

			TestSpec
				.forString("TIMESTAMP(3)")
				.expectType(new TimestampType(3)),

			TestSpec
				.forString("TIMESTAMP WITHOUT TIME ZONE")
				.expectType(new TimestampType()),

			TestSpec
				.forString("TIMESTAMP(3) WITHOUT TIME ZONE")
				.expectType(new TimestampType(3)),

			TestSpec
				.forString("TIMESTAMP WITH TIME ZONE")
				.expectType(new ZonedTimestampType()),

			TestSpec
				.forString("TIMESTAMP(3) WITH TIME ZONE")
				.expectType(new ZonedTimestampType(3)),

			TestSpec
				.forString("TIMESTAMP WITH LOCAL TIME ZONE")
				.expectType(new LocalZonedTimestampType()),

			TestSpec
				.forString("TIMESTAMP(3) WITH LOCAL TIME ZONE")
				.expectType(new LocalZonedTimestampType(3)),

			TestSpec
				.forString("INTERVAL YEAR")
				.expectType(new YearMonthIntervalType(YearMonthResolution.YEAR)),

			TestSpec
				.forString("INTERVAL YEAR(4)")
				.expectType(new YearMonthIntervalType(YearMonthResolution.YEAR, 4)),

			TestSpec
				.forString("INTERVAL MONTH")
				.expectType(new YearMonthIntervalType(YearMonthResolution.MONTH)),

			TestSpec
				.forString("INTERVAL YEAR TO MONTH")
				.expectType(new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH)),

			TestSpec
				.forString("INTERVAL YEAR(4) TO MONTH")
				.expectType(new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH, 4)),

			TestSpec
				.forString("INTERVAL DAY(2) TO SECOND(3)")
				.expectType(new DayTimeIntervalType(DayTimeResolution.DAY_TO_SECOND, 2, 3)),

			TestSpec
				.forString("INTERVAL HOUR TO SECOND(3)")
				.expectType(
					new DayTimeIntervalType(
						DayTimeResolution.HOUR_TO_SECOND,
						DayTimeIntervalType.DEFAULT_DAY_PRECISION,
						3)
				),

			TestSpec
				.forString("INTERVAL MINUTE")
				.expectType(new DayTimeIntervalType(DayTimeResolution.MINUTE)),

			TestSpec
				.forString("ARRAY<TIMESTAMP(3) WITH LOCAL TIME ZONE>")
				.expectType(new ArrayType(new LocalZonedTimestampType(3))),

			TestSpec
				.forString("ARRAY<INT NOT NULL>")
				.expectType(new ArrayType(new IntType(false))),

			TestSpec
				.forString("INT ARRAY")
				.expectType(new ArrayType(new IntType())),

			TestSpec
				.forString("INT NOT NULL ARRAY")
				.expectType(new ArrayType(new IntType(false))),

			TestSpec
				.forString("INT ARRAY NOT NULL")
				.expectType(new ArrayType(false, new IntType())),

			TestSpec
				.forString("MULTISET<INT NOT NULL>")
				.expectType(new MultisetType(new IntType(false))),

			TestSpec
				.forString("INT MULTISET")
				.expectType(new MultisetType(new IntType())),

			TestSpec
				.forString("INT NOT NULL MULTISET")
				.expectType(new MultisetType(new IntType(false))),

			TestSpec
				.forString("INT MULTISET NOT NULL")
				.expectType(new MultisetType(false, new IntType())),

			TestSpec
				.forString("MAP<BIGINT, BOOLEAN>")
				.expectType(new MapType(new BigIntType(), new BooleanType())),

			TestSpec
				.forString("ROW<f0 INT NOT NULL, f1 BOOLEAN>")
				.expectType(
					new RowType(
						Arrays.asList(
							new RowType.RowField("f0", new IntType(false)),
							new RowType.RowField("f1", new BooleanType())))
				),

			TestSpec
				.forString("ROW(f0 INT NOT NULL, f1 BOOLEAN)")
				.expectType(
					new RowType(
						Arrays.asList(
							new RowType.RowField("f0", new IntType(false)),
							new RowType.RowField("f1", new BooleanType())))
				),

			TestSpec
				.forString("ROW<`f0` INT>")
				.expectType(
					new RowType(
						Collections.singletonList(new RowType.RowField("f0", new IntType())))
				),

			TestSpec
				.forString("ROW(`f0` INT)")
				.expectType(
					new RowType(
						Collections.singletonList(new RowType.RowField("f0", new IntType())))
				),

			TestSpec
				.forString("ROW<>")
				.expectType(new RowType(Collections.emptyList())),

			TestSpec
				.forString("ROW()")
				.expectType(new RowType(Collections.emptyList())),

			TestSpec
				.forString("ROW<f0 INT NOT NULL 'This is a comment.', f1 BOOLEAN 'This as well.'>")
				.expectType(
					new RowType(
						Arrays.asList(
							new RowType.RowField("f0", new IntType(false), "This is a comment."),
							new RowType.RowField("f1", new BooleanType(), "This as well.")))
				),

			TestSpec
				.forString("NULL")
				.expectType(new NullType()),

			TestSpec
				.forString(createAnyType(LogicalTypeParserTest.class).asSerializableString())
				.expectType(createAnyType(LogicalTypeParserTest.class)),

			TestSpec
				.forString("cat.db.MyType")
				.expectType(new UnresolvedUserDefinedType("cat", "db", "MyType")),

			TestSpec
				.forString("`db`.`MyType`")
				.expectType(new UnresolvedUserDefinedType(null, "db", "MyType")),

			TestSpec
				.forString("MyType")
				.expectType(new UnresolvedUserDefinedType(null, null, "MyType")),

			TestSpec
				.forString("ARRAY<MyType>")
				.expectType(new ArrayType(new UnresolvedUserDefinedType(null, null, "MyType"))),

			TestSpec
				.forString("ROW<f0 MyType, f1 `c`.`d`.`t`>")
				.expectType(
					RowType.of(
						new UnresolvedUserDefinedType(null, null, "MyType"),
						new UnresolvedUserDefinedType("c", "d", "t"))
				),

			// error message testing

			TestSpec
				.forString("ROW<`f0")
				.expectErrorMessage("Unexpected end"),

			TestSpec
				.forString("ROW<`f0`")
				.expectErrorMessage("Unexpected end"),

			TestSpec
				.forString("VARCHAR(test)")
				.expectErrorMessage("<LITERAL_INT> expected"),

			TestSpec
				.forString("VARCHAR(33333333333)")
				.expectErrorMessage("Invalid integer value"),

			TestSpec
				.forString("ROW<field INT, field2>")
				.expectErrorMessage("<KEYWORD> expected"),

			TestSpec
				.forString("ANY('unknown.class', '')")
				.expectErrorMessage("Unable to restore the ANY type")
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testParsing() {
		if (testSpec.expectedType != null) {
			assertThat(
				LogicalTypeParser.parse(testSpec.typeString),
				equalTo(testSpec.expectedType));
		}
	}

	@Test
	public void testSerializableParsing() {
		if (testSpec.expectedType != null) {
			if (!hasRoot(testSpec.expectedType, UNRESOLVED) &&
					testSpec.expectedType.getChildren().stream().noneMatch(t -> hasRoot(t, UNRESOLVED))) {
				assertThat(
					LogicalTypeParser.parse(testSpec.expectedType.asSerializableString()),
					equalTo(testSpec.expectedType));
			}
		}
	}

	@Test
	public void testErrorMessage() {
		if (testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectMessage(testSpec.expectedErrorMessage);

			LogicalTypeParser.parse(testSpec.typeString);
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		private final String typeString;

		private @Nullable LogicalType expectedType;

		private @Nullable String expectedErrorMessage;

		private TestSpec(String typeString) {
			this.typeString = typeString;
		}

		static TestSpec forString(String typeString) {
			return new TestSpec(typeString);
		}

		TestSpec expectType(LogicalType expectedType) {
			this.expectedType = expectedType;
			return this;
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}
	}

	private static <T> AnyType<T> createAnyType(Class<T> clazz) {
		return new AnyType<>(clazz, new KryoSerializer<>(clazz, new ExecutionConfig()));
	}
}
