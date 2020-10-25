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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link KeyExtractor}.
 */
public class KeyExtractorTest {
	@Test
	public void testSimpleKey() {
		TableSchema schema = TableSchema.builder()
			.field("a", DataTypes.BIGINT().notNull())
			.field("b", DataTypes.STRING())
			.primaryKey("a")
			.build();

		Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");

		String key = keyExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
		assertThat(key, equalTo("12"));
	}

	@Test
	public void testNoPrimaryKey() {
		TableSchema schema = TableSchema.builder()
			.field("a", DataTypes.BIGINT().notNull())
			.field("b", DataTypes.STRING())
			.build();

		Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");

		String key = keyExtractor.apply(GenericRowData.of(12L, StringData.fromString("ABCD")));
		assertThat(key, nullValue());
	}

	@Test
	public void testTwoFieldsKey() {
		TableSchema schema = TableSchema.builder()
			.field("a", DataTypes.BIGINT().notNull())
			.field("b", DataTypes.STRING())
			.field("c", DataTypes.TIMESTAMP().notNull())
			.primaryKey("a", "c")
			.build();

		Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");

		String key = keyExtractor.apply(
			GenericRowData.of(
				12L,
				StringData.fromString("ABCD"),
				TimestampData.fromLocalDateTime(LocalDateTime.parse("2012-12-12T12:12:12"))
			));
		assertThat(key, equalTo("12_2012-12-12T12:12:12"));
	}

	@Test
	public void testAllTypesKey() {
		TableSchema schema = TableSchema.builder()
			.field("a", DataTypes.TINYINT().notNull())
			.field("b", DataTypes.SMALLINT().notNull())
			.field("c", DataTypes.INT().notNull())
			.field("d", DataTypes.BIGINT().notNull())
			.field("e", DataTypes.BOOLEAN().notNull())
			.field("f", DataTypes.FLOAT().notNull())
			.field("g", DataTypes.DOUBLE().notNull())
			.field("h", DataTypes.STRING().notNull())
			.field("i", DataTypes.TIMESTAMP().notNull())
			.field("j", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull())
			.field("k", DataTypes.TIME().notNull())
			.field("l", DataTypes.DATE().notNull())
			.primaryKey("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l")
			.build();

		Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");

		String key = keyExtractor.apply(
			GenericRowData.of(
				(byte) 1,
				(short) 2,
				3,
				(long) 4,
				true,
				1.0f,
				2.0d,
				StringData.fromString("ABCD"),
				TimestampData.fromLocalDateTime(LocalDateTime.parse("2012-12-12T12:12:12")),
				TimestampData.fromInstant(Instant.parse("2013-01-13T13:13:13Z")),
				(int) (LocalTime.parse("14:14:14").toNanoOfDay() / 1_000_000),
				(int) LocalDate.parse("2015-05-15").toEpochDay()
			));
		assertThat(
			key,
			equalTo("1_2_3_4_true_1.0_2.0_ABCD_2012-12-12T12:12:12_2013-01-13T13:13:13_14:14:14_2015-05-15"));
	}
}
