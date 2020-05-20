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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withColumns;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * End to end tests for {@link org.apache.flink.table.api.TableEnvironment#fromValues}.
 */
public class ValuesITCase extends StreamingTestBase {
	@Test
	public void testTypeConversions() throws Exception {
		List<Row> data = Arrays.asList(
			Row.of(
				1,
				"ABC",
				java.sql.Timestamp.valueOf("2000-12-12 12:30:57.12"),
				Row.of(1, new byte[]{1, 2}, "ABC", Arrays.asList(1, 2, 3))),
			Row.of(
				Math.PI,
				"ABC",
				LocalDateTime.parse("2000-12-12T12:30:57.123456"),
				Row.of(Math.PI, new byte[]{2, 3}, "ABC", Arrays.asList(1L, 2L, 3L))),
			Row.of(
				3.1f,
				"DEF",
				LocalDateTime.parse("2000-12-12T12:30:57.1234567"),
				Row.of(3.1f, new byte[]{3}, "DEF", Arrays.asList(1D, 2D, 3D))),
			Row.of(
				99L,
				"DEFG",
				LocalDateTime.parse("2000-12-12T12:30:57.12345678"),
				Row.of(99L, new byte[]{3, 4}, "DEFG", Arrays.asList(1f, 2f, 3f))),
			Row.of(
				0d,
				"D",
				LocalDateTime.parse("2000-12-12T12:30:57.123"),
				Row.of(0d, new byte[]{4}, "D", Arrays.asList(1, 2, 3)))
		);

		DataType rowType = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.DECIMAL(10, 2).notNull()),
			DataTypes.FIELD("b", DataTypes.CHAR(4).notNull()),
			DataTypes.FIELD("c", DataTypes.TIMESTAMP(4).notNull()),
			DataTypes.FIELD(
				"row",
				DataTypes.ROW(
					DataTypes.FIELD("a", DataTypes.DECIMAL(10, 3)),
					DataTypes.FIELD("b", DataTypes.BINARY(2)),
					DataTypes.FIELD("c", DataTypes.CHAR(5).notNull()),
					DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.DECIMAL(10, 2))))
			)
		);

		Table t = tEnv().fromValues(
			rowType,
			data
		);

		TestCollectionTableFactory.reset();
		tEnv().executeSql(
			"CREATE TABLE SinkTable(" +
				"a DECIMAL(10, 2) NOT NULL, " +
				"b CHAR(4) NOT NULL," +
				"c TIMESTAMP(4) NOT NULL," +
				"`row` ROW<a DECIMAL(10, 3) NOT NULL, b BINARY(2), c CHAR(5) NOT NULL, d ARRAY<DECIMAL(10, 2)>>) " +
				"WITH ('connector' = 'COLLECTION')");
		TableResult tableResult = t.executeInsert("SinkTable");
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		List<Row> expected = Arrays.asList(
			Row.of(
				new BigDecimal("1.00"),
				"ABC ",
				LocalDateTime.parse("2000-12-12T12:30:57.120"),
				Row.of(new BigDecimal("1.000"),
					new byte[]{1, 2},
					"ABC  ",
					new BigDecimal[]{new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")})),
			Row.of(
				new BigDecimal("3.14"),
				"ABC ",
				LocalDateTime.parse("2000-12-12T12:30:57.123400"),
				Row.of(new BigDecimal("3.142"),
					new byte[]{2, 3},
					"ABC  ",
					new BigDecimal[]{new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")})),
			Row.of(
				new BigDecimal("3.10"),
				"DEF ",
				LocalDateTime.parse("2000-12-12T12:30:57.123400"),
				Row.of(new BigDecimal("3.100"),
					new byte[]{3, 0},
					"DEF  ",
					new BigDecimal[]{new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")})),
			Row.of(
				new BigDecimal("99.00"),
				"DEFG",
				LocalDateTime.parse("2000-12-12T12:30:57.123400"),
				Row.of(new BigDecimal("99.000"),
					new byte[]{3, 4},
					"DEFG ",
					new BigDecimal[]{new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")})),
			Row.of(
				new BigDecimal("0.00"),
				"D   ",
				LocalDateTime.parse("2000-12-12T12:30:57.123"),
				Row.of(new BigDecimal("0.000"),
					new byte[]{4, 0},
					"D    ",
					new BigDecimal[]{new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")}))
		);

		List<Row> actual = TestCollectionTableFactory.getResult();
		assertThat(
			new HashSet<>(actual),
			equalTo(new HashSet<>(expected)));
	}

	@Test
	public void testAllTypes() throws Exception {
		List<Row> data = Arrays.asList(
			rowWithNestedRow(
				(byte) 1,
				(short) 1,
				1,
				1L,
				1.1f,
				1.1,
				new BigDecimal("1.1"),
				true,
				LocalTime.of(1, 1, 1),
				LocalDate.of(1, 1, 1),
				LocalDateTime.of(1, 1, 1, 1, 1, 1, 1),
				Instant.ofEpochMilli(1),
				"1",
				new byte[] {1},
				new BigDecimal[]{new BigDecimal("1.1")},
				createMap("1", new BigDecimal("1.1"))
			),
			rowWithNestedRow(
				(byte) 2,
				(short) 2,
				2,
				2L,
				2.2f,
				2.2,
				new BigDecimal("2.2"),
				false,
				LocalTime.of(2, 2, 2),
				LocalDate.of(2, 2, 2),
				LocalDateTime.of(2, 2, 2, 2, 2, 2, 2),
				Instant.ofEpochMilli(2),
				"2",
				new byte[] {2},
				new BigDecimal[]{new BigDecimal("2.2")},
				createMap("2", new BigDecimal("2.2"))
			)
		);

		Table t = tEnv().fromValues(data);

		TestCollectionTableFactory.reset();
		tEnv().executeSql(
			"CREATE TABLE SinkTable(" +
				"f0 TINYINT, " +
				"f1 SMALLINT, " +
				"f2 INT, " +
				"f3 BIGINT, " +
				"f4 FLOAT, " +
				"f5 DOUBLE, " +
				"f6 DECIMAL(2, 1), " +
				"f7 BOOLEAN, " +
				"f8 TIME(0), " +
				"f9 DATE, " +
				"f12 TIMESTAMP(9), " +
				"f13 TIMESTAMP(3) WITH LOCAL TIME ZONE, " +
				"f14 CHAR(1), " +
				"f15 BINARY(1), " +
				"f16 ARRAY<DECIMAL(2, 1)>, " +
				"f17 MAP<CHAR(1), DECIMAL(2, 1)>, " +
				"f18 ROW<" +
				"   `f0` TINYINT, " +
				"   `f1` SMALLINT, " +
				"   `f2` INT, " +
				"   `f3` BIGINT, " +
				"   `f4` FLOAT, " +
				"   `f5` DOUBLE, " +
				"   `f6` DECIMAL(2, 1), " +
				"   `f7` BOOLEAN, " +
				"   `f8` TIME(0), " +
				"   `f9` DATE, " +
				"   `f12` TIMESTAMP(9), " +
				"   `f13` TIMESTAMP(3) WITH LOCAL TIME ZONE, " +
				"   `f14` CHAR(1), " +
				"   `f15` BINARY(1), " +
				"   `f16` ARRAY<DECIMAL(2, 1)>, " +
				"   `f17` MAP<CHAR(1), DECIMAL(2, 1)>>) " +
				"WITH ('connector' = 'COLLECTION')");
		execInsertTableAndWaitResult(t, "SinkTable");

		List<Row> actual = TestCollectionTableFactory.getResult();
		assertThat(
			new HashSet<>(actual),
			equalTo(new HashSet<>(data)));
	}

	@Test
	public void testProjectionWithValues() throws Exception {
		List<Row> data = Arrays.asList(
			Row.of(
				(byte) 1,
				(short) 1,
				1,
				1L,
				1.1f,
				1.1,
				new BigDecimal("1.1"),
				true,
				LocalTime.of(1, 1, 1),
				LocalDate.of(1, 1, 1),
				LocalDateTime.of(1, 1, 1, 1, 1, 1, 1),
				Instant.ofEpochMilli(1),
				"1",
				new byte[] {1},
				new BigDecimal[]{new BigDecimal("1.1")},
				createMap("1", new BigDecimal("1.1"))
			),
			Row.of(
				(byte) 2,
				(short) 2,
				2,
				2L,
				2.2f,
				2.2,
				new BigDecimal("2.2"),
				false,
				LocalTime.of(2, 2, 2),
				LocalDate.of(2, 2, 2),
				LocalDateTime.of(2, 2, 2, 2, 2, 2, 2),
				Instant.ofEpochMilli(2),
				"2",
				new byte[] {2},
				new BigDecimal[]{new BigDecimal("2.2")},
				createMap("2", new BigDecimal("2.2"))
			)
		);

		tEnv().createTemporaryFunction("func", new CustomScalarFunction());
		Table t = tEnv().fromValues(data)
			.select(call("func", withColumns(range("f0", "f15"))));

		TestCollectionTableFactory.reset();
		tEnv().executeSql(
			"CREATE TABLE SinkTable(str STRING) WITH ('connector' = 'COLLECTION')");
		TableEnvUtil.execInsertTableAndWaitResult(t, "SinkTable");

		List<Row> actual = TestCollectionTableFactory.getResult();
		List<Row> expected = Arrays.asList(
			Row.of(
				"1,1,1,1,1.1,1.1,1.1,true,01:01:01,0001-01-01,0001-01-01T01:01:01.000000001," +
					"1970-01-01T00:00:00.001Z,1,[1],[1.1],{1=1.1}"),
			Row.of(
				"2,2,2,2,2.2,2.2,2.2,false,02:02:02,0002-02-02,0002-02-02T02:02:02.000000002," +
					"1970-01-01T00:00:00.002Z,2,[2],[2.2],{2=2.2}")
		);
		assertThat(
			new HashSet<>(actual),
			equalTo(new HashSet<>(expected)));
	}

	@Test
	public void testRegisteringValuesWithComplexTypes() {
		Map<Integer, Integer> mapData = new HashMap<>();
		mapData.put(1, 1);
		mapData.put(2, 2);

		Row row = Row.of(mapData, Row.of(1, 2, 3), new Integer[]{1, 2});
		Table values = tEnv().fromValues(Collections.singletonList(row));
		tEnv().createTemporaryView("values_t", values);
		Iterator<Row> iter = tEnv().executeSql("select * from values_t").collect();

		List<Row> results = new ArrayList<>();
		iter.forEachRemaining(results::add);
		assertThat(results, equalTo(Collections.singletonList(row)));
	}

	/**
	 * A {@link ScalarFunction} that takes all supported types as parameters and
	 * converts them to String.
	 */
	@FunctionHint(
		output = @DataTypeHint("STRING"),
		input = {
			@DataTypeHint("TINYINT"),
			@DataTypeHint("SMALLINT"),
			@DataTypeHint("INT"),
			@DataTypeHint("BIGINT"),
			@DataTypeHint("FLOAT"),
			@DataTypeHint("DOUBLE"),
			@DataTypeHint("DECIMAL(2, 1)"),
			@DataTypeHint("BOOLEAN"),
			@DataTypeHint("TIME(0)"),
			@DataTypeHint("DATE"),
			@DataTypeHint("TIMESTAMP(9)"),
			@DataTypeHint("TIMESTAMP(3) WITH LOCAL TIME ZONE"),
			@DataTypeHint("CHAR(1)"),
			@DataTypeHint("BINARY(1)"),
			@DataTypeHint("ARRAY<DECIMAL(2, 1)>"),
			@DataTypeHint("MAP<CHAR(1), DECIMAL(2, 1)>")
		}
	)
	public static class CustomScalarFunction extends ScalarFunction {
		public String eval(
				byte tinyint,
				short smallInt,
				int integer,
				long bigint,
				float floating,
				double doublePrecision,
				BigDecimal decimal,
				boolean bool,
				LocalTime time,
				LocalDate date,
//				Period dateTimeInteraval, TODO TIMESTAMP WITH TIMEZONE not supported yet
//				Duration timeInterval, TODO TIMESTAMP WITH TIMEZONE not supported yet
//				OffsetDateTime zonedDateTime, TODO TIMESTAMP WITH TIMEZONE not supported yet
				LocalDateTime timestamp,
				Instant localZonedTimestamp,
				String character,
				byte[] binary,
				BigDecimal[] array,
				Map<String, BigDecimal> map) {
			return Stream.of(
				tinyint,
				smallInt,
				integer,
				bigint,
				floating,
				doublePrecision,
				decimal,
				bool,
				time,
				date,
				timestamp,
				localZonedTimestamp,
				character,
				Arrays.toString(binary),
				Arrays.toString(array),
				map
			).map(Object::toString)
				.collect(Collectors.joining(","));
		}
	}

	private static Map<String, BigDecimal> createMap(String key, BigDecimal value) {
		Map<String, BigDecimal> map = new HashMap<>();
		map.put(key, value);
		return map;
	}

	private static Row rowWithNestedRow(
			byte tinyint,
			short smallInt,
			int integer,
			long bigint,
			float floating,
			double doublePrecision,
			BigDecimal decimal,
			boolean bool,
			LocalTime time,
			LocalDate date,
//			Period dateTimeInteraval, // TODO INTERVAL types not supported yet
//			Duration timeInterval, // TODO INTERVAL types not supported yet
//			OffsetDateTime zonedDateTime, TODO TIMESTAMP WITH TIMEZONE not supported yet
			LocalDateTime timestamp,
			Instant localZonedTimestamp,
			String character,
			byte[] binary,
			BigDecimal[] array,
			Map<String, BigDecimal> map) {
		return Row.of(
			tinyint,
			smallInt,
			integer,
			bigint,
			floating,
			doublePrecision,
			decimal,
			bool,
			time,
			date,
			timestamp,
			localZonedTimestamp,
			character,
			binary,
			array,
			map,
			Row.of(
				tinyint,
				smallInt,
				integer,
				bigint,
				floating,
				doublePrecision,
				decimal,
				bool,
				time,
				date,
				timestamp,
				localZonedTimestamp,
				character,
				binary,
				array,
				map
			)
		);
	}
}
