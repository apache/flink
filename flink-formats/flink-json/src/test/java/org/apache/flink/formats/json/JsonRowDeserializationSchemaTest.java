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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.formats.utils.DeserializationSchemaMatcher.whenDeserializedWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;

/**
 * Tests for the {@link JsonRowDeserializationSchema}.
 */
public class JsonRowDeserializationSchemaTest {

	/**
	 * Tests simple deserialization using type information.
	 */
	@Test
	public void testTypeInfoDeserialization() throws Exception {
		long id = 1238123899121L;
		String name = "asdlkjasjkdla998y1122";
		byte[] bytes = new byte[1024];
		ThreadLocalRandom.current().nextBytes(bytes);
		Timestamp timestamp = Timestamp.valueOf("1990-10-14 12:12:43");
		Date date = Date.valueOf("1990-10-14");
		Time time = Time.valueOf("12:12:43");

		Map<String, Long> map = new HashMap<>();
		map.put("flink", 123L);

		Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
		Map<String, Integer> innerMap = new HashMap<>();
		innerMap.put("key", 234);
		nestedMap.put("inner_map", innerMap);

		ObjectMapper objectMapper = new ObjectMapper();

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", id);
		root.put("name", name);
		root.put("bytes", bytes);
		root.put("date1", "1990-10-14");
		root.put("date2", "1990-10-14");
		root.put("time1", "12:12:43Z");
		root.put("time2", "12:12:43Z");
		root.put("timestamp1", "1990-10-14T12:12:43Z");
		root.put("timestamp2", "1990-10-14T12:12:43Z");
		root.putObject("map").put("flink", 123);
		root.putObject("map2map").putObject("inner_map").put("key", 234);

		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(
			Types.ROW_NAMED(
				new String[]{"id", "name", "bytes", "date1", "date2",
					"time1", "time2", "timestamp1", "timestamp2", "map", "map2map"},
				Types.LONG, Types.STRING, Types.PRIMITIVE_ARRAY(Types.BYTE),
				Types.SQL_DATE, Types.LOCAL_DATE, Types.SQL_TIME, Types.LOCAL_TIME,
				Types.SQL_TIMESTAMP, Types.LOCAL_DATE_TIME,
				Types.MAP(Types.STRING, Types.LONG),
				Types.MAP(Types.STRING, Types.MAP(Types.STRING, Types.INT)))
		).build();

		Row row = new Row(11);
		row.setField(0, id);
		row.setField(1, name);
		row.setField(2, bytes);
		row.setField(3, date);
		row.setField(4, date.toLocalDate());
		row.setField(5, time);
		row.setField(6, time.toLocalTime());
		row.setField(7, timestamp);
		row.setField(8, timestamp.toLocalDateTime());
		row.setField(9, map);
		row.setField(10, nestedMap);

		assertThat(serializedJson, whenDeserializedWith(deserializationSchema).equalsTo(row));
	}

	@Test
	public void testSchemaDeserialization() throws Exception {
		final BigDecimal id = BigDecimal.valueOf(1238123899121L);
		final String name = "asdlkjasjkdla998y1122";
		final byte[] bytes = new byte[1024];
		ThreadLocalRandom.current().nextBytes(bytes);
		final BigDecimal[] numbers = new BigDecimal[] {
			BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(3)};
		final String[] strings = new String[] {"one", "two", "three"};

		final ObjectMapper objectMapper = new ObjectMapper();

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", id.longValue());
		root.putNull("idOrNull");
		root.put("name", name);
		root.put("date", "1990-10-14");
		root.put("time", "12:12:43Z");
		root.put("timestamp", "1990-10-14T12:12:43Z");
		root.put("bytes", bytes);
		root.putArray("numbers").add(1).add(2).add(3);
		root.putArray("strings").add("one").add("two").add("three");
		root.putObject("nested").put("booleanField", true).put("decimalField", 12);

		final byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(
			"{" +
			"    type: 'object'," +
			"    properties: {" +
			"         id: { type: 'integer' }," +
			"         idOrNull: { type: ['integer', 'null'] }," +
			"         name: { type: 'string' }," +
			"         date: { type: 'string', format: 'date' }," +
			"         time: { type: 'string', format: 'time' }," +
			"         timestamp: { type: 'string', format: 'date-time' }," +
			"         bytes: { type: 'string', contentEncoding: 'base64' }," +
			"         numbers: { type: 'array', items: { type: 'integer' } }," +
			"         strings: { type: 'array', items: { type: 'string' } }," +
			"         nested: { " +
			"             type: 'object'," +
			"             properties: { " +
			"                 booleanField: { type: 'boolean' }," +
			"                 decimalField: { type: 'number' }" +
			"             }" +
			"         }" +
			"    }" +
			"}").build();

		final Row expected = new Row(10);
		expected.setField(0, id);
		expected.setField(1, null);
		expected.setField(2, name);
		expected.setField(3, Date.valueOf("1990-10-14"));
		expected.setField(4, Time.valueOf("12:12:43"));
		expected.setField(5, Timestamp.valueOf("1990-10-14 12:12:43"));
		expected.setField(6, bytes);
		expected.setField(7, numbers);
		expected.setField(8, strings);
		final Row nestedRow = new Row(2);
		nestedRow.setField(0, true);
		nestedRow.setField(1, BigDecimal.valueOf(12));
		expected.setField(9, nestedRow);

		assertThat(serializedJson, whenDeserializedWith(deserializationSchema).equalsTo(expected));
	}

	/**
	 * Tests deserialization with non-existing field name.
	 */
	@Test
	public void testMissingNode() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", 123123123);
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(
			new String[]{"name"},
			Types.STRING);

		JsonRowDeserializationSchema deserializationSchema =
			new JsonRowDeserializationSchema.Builder(rowTypeInformation)
				.build();

		Row row = new Row(1);
		assertThat(serializedJson,
			whenDeserializedWith(deserializationSchema).equalsTo(row));

		deserializationSchema = new JsonRowDeserializationSchema.Builder(rowTypeInformation)
			.failOnMissingField()
			.build();

		assertThat(serializedJson,
			whenDeserializedWith(deserializationSchema)
				.failsWithException(hasCause(instanceOf(IllegalStateException.class))));
	}

	/**
	 * Tests that number of field names and types has to match.
	 */
	@Test
	public void testNumberOfFieldNamesAndTypesMismatch() {
		try {
			new JsonRowDeserializationSchema.Builder(
				Types.ROW_NAMED(
					new String[]{"one", "two", "three"},
					Types.LONG)).build();
			Assert.fail("Did not throw expected Exception");
		} catch (IllegalArgumentException ignored) {
			// Expected
		}
	}
}
