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

package org.apache.flink.streaming.connectors.elasticsearch.index;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.ArrayList;
import java.util.List;

/**
 * Suite tests for {@link IndexGenerator}.
 */
public class IndexGeneratorTest {

	private TableSchema schema;
	private List<Row> rows;

	@Before
	public void prepareData() {
		String[] fieldNames = new String[]{
			"id",
			"item",
			"log_ts",
			"log_date",
			"order_timestamp",
			"log_time",
			"local_datetime",
			"local_date",
			"local_time",
			"note",
			"status"};
		DataType[] dataTypes = new DataType[]{
			DataTypes.INT(),
			DataTypes.STRING(),
			DataTypes.BIGINT(),
			DataTypes.DATE().bridgedTo(java.sql.Date.class),
			DataTypes.TIMESTAMP().bridgedTo(java.sql.Timestamp.class),
			DataTypes.TIME().bridgedTo(java.sql.Time.class),
			DataTypes.TIMESTAMP().bridgedTo(java.time.LocalDateTime.class),
			DataTypes.DATE().bridgedTo(java.time.LocalDate.class),
			DataTypes.TIME().bridgedTo(java.time.LocalTime.class),
			DataTypes.STRING(),
			DataTypes.BOOLEAN()
		};
		schema = new TableSchema.Builder().fields(fieldNames, dataTypes).build();

		rows = new ArrayList<>();
		rows.add(Row.of(
			1,
			"apple",
			Timestamp.valueOf("2020-03-18 12:12:14").getTime(),
			Date.valueOf("2020-03-18"),
			Timestamp.valueOf("2020-03-18 12:12:14"),
			Time.valueOf("12:12:14"),
			LocalDateTime.of(2020, 3, 18, 12, 12, 14, 1000),
			LocalDate.of(2020, 3, 18),
			LocalTime.of(12, 13, 14, 2000),
			"test1",
			true));
		rows.add(Row.of(
			2,
			"peanut",
			Timestamp.valueOf("2020-03-19 12:22:14").getTime(),
			Date.valueOf("2020-03-19"),
			Timestamp.valueOf("2020-03-19 12:22:21"),
			Time.valueOf("12:22:21"),
			LocalDateTime.of(2020, 3, 19, 12, 22, 14, 1000),
			LocalDate.of(2020, 3, 19),
			LocalTime.of(12, 13, 14, 2000),
			"test2",
			false));
	}

	@Test
	public void testDynamicIndexFromTimestamp() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"{order_timestamp|yyyy_MM_dd_HH-ss}_index", schema);
		indexGenerator.open();
		Assert.assertEquals("2020_03_18_12-14_index", indexGenerator.generate(rows.get(0)));
		IndexGenerator indexGenerator1 = IndexGeneratorFactory.createIndexGenerator(
			"{order_timestamp|yyyy_MM_dd_HH_mm}_index", schema);
		indexGenerator1.open();
		Assert.assertEquals("2020_03_19_12_22_index", indexGenerator1.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromLocalDateTime() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"{local_datetime|yyyy_MM_dd_HH-ss}_index", schema);
		indexGenerator.open();
		Assert.assertEquals("2020_03_18_12-14_index", indexGenerator.generate(rows.get(0)));
		IndexGenerator indexGenerator1 = IndexGeneratorFactory.createIndexGenerator(
			"{local_datetime|yyyy_MM_dd_HH_mm}_index", schema);
		indexGenerator1.open();
		Assert.assertEquals("2020_03_19_12_22_index", indexGenerator1.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromDate() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{log_date|yyyy/MM/dd}", schema);
		indexGenerator.open();
		Assert.assertEquals("my-index-2020/03/18", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-2020/03/19", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromLocalDate() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{local_date|yyyy/MM/dd}", schema);
		indexGenerator.open();
		Assert.assertEquals("my-index-2020/03/18", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-2020/03/19", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromTime() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{log_time|HH-mm}", schema);
		indexGenerator.open();
		Assert.assertEquals("my-index-12-12", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-12-22", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromLocalTime() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{local_time|HH-mm}", schema);
		indexGenerator.open();
		Assert.assertEquals("my-index-12-13", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-12-13", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testDynamicIndexDefaultFormat() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index-{local_time|}", schema);
		indexGenerator.open();
		Assert.assertEquals("my-index-12_13_14", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index-12_13_14", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testGeneralDynamicIndex() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"index_{item}", schema);
		indexGenerator.open();
		Assert.assertEquals("index_apple", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("index_peanut", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testStaticIndex() {
		IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
			"my-index", schema);
		indexGenerator.open();
		Assert.assertEquals("my-index", indexGenerator.generate(rows.get(0)));
		Assert.assertEquals("my-index", indexGenerator.generate(rows.get(1)));
	}

	@Test
	public void testUnknownField() {
		String expectedExceptionMsg = "Unknown field 'unknown_ts' in index pattern 'my-index-{unknown_ts|yyyy-MM-dd}'," +
			" please check the field name.";
		try {
			IndexGeneratorFactory.createIndexGenerator("my-index-{unknown_ts|yyyy-MM-dd}", schema);
		} catch (TableException e) {
			Assert.assertEquals(e.getMessage(), expectedExceptionMsg);
		}
	}

	@Test
	public void testUnsupportedTimeType() {
		String expectedExceptionMsg = "Unsupported type 'INT' found in Elasticsearch dynamic index field, " +
			"time-related pattern only support types are: DATE,TIME,TIMESTAMP.";
		try {
			IndexGeneratorFactory.createIndexGenerator("my-index-{id|yyyy-MM-dd}", schema);
		} catch (TableException e) {
			Assert.assertEquals(expectedExceptionMsg, e.getMessage());
		}
	}

	@Test
	public void testUnsupportedMultiParametersType() {
		String expectedExceptionMsg = "Chaining dynamic index pattern my-index-{local_date}-{local_time} is not supported," +
			" only support single dynamic index pattern.";
		try {
			IndexGeneratorFactory.createIndexGenerator("my-index-{local_date}-{local_time}", schema);
		} catch (TableException e) {
			Assert.assertEquals(expectedExceptionMsg, e.getMessage());
		}
	}

	@Test
	public void testDynamicIndexUnsupportedFormat() {
		String expectedExceptionMsg = "Unsupported field: HourOfDay";
		try {
			IndexGeneratorFactory.createIndexGenerator(
				"my-index-{local_date|yyyy/MM/dd HH:mm}", schema);
		} catch (UnsupportedTemporalTypeException e) {
			Assert.assertEquals(expectedExceptionMsg, e.getMessage());
		}
	}

	@Test
	public void testUnsupportedIndexFieldType() {
		String expectedExceptionMsg = "Unsupported type Boolean of index field, Supported types are:" +
			" [LocalDateTime, Timestamp, LocalDate, Date, LocalTime, Time, String, Short, Integer, Long]";
		try {
			IndexGeneratorFactory.createIndexGenerator("index_{status}", schema);
		} catch (IllegalArgumentException e) {
			Assert.assertEquals(expectedExceptionMsg, e.getMessage());
		}
	}
}
