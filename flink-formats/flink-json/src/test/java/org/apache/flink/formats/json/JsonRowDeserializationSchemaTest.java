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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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

		ObjectMapper objectMapper = new ObjectMapper();

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", id);
		root.put("name", name);
		root.put("bytes", bytes);

		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(
			Types.ROW_NAMED(
				new String[] { "id", "name", "bytes" },
				Types.LONG, Types.STRING, Types.PRIMITIVE_ARRAY(Types.BYTE))
		);

		Row deserialized = deserializationSchema.deserialize(serializedJson);

		assertEquals(3, deserialized.getArity());
		assertEquals(id, deserialized.getField(0));
		assertEquals(name, deserialized.getField(1));
		assertArrayEquals(bytes, (byte[]) deserialized.getField(2));
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

		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(
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
			"}");

		final Row deserialized = deserializationSchema.deserialize(serializedJson);

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

		assertEquals(expected, deserialized);
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

		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(
			Types.ROW_NAMED(
				new String[] { "name" },
				Types.STRING)
		);

		Row row = deserializationSchema.deserialize(serializedJson);

		assertEquals(1, row.getArity());
		Assert.assertNull("Missing field not null", row.getField(0));

		deserializationSchema.setFailOnMissingField(true);

		try {
			deserializationSchema.deserialize(serializedJson);
			Assert.fail("Did not throw expected Exception");
		} catch (IOException e) {
			Assert.assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}

	/**
	 * Tests that number of field names and types has to match.
	 */
	@Test
	public void testNumberOfFieldNamesAndTypesMismatch() {
		try {
			new JsonRowDeserializationSchema(
				Types.ROW_NAMED(
					new String[]{"one", "two", "three"},
					Types.LONG));
			Assert.fail("Did not throw expected Exception");
		} catch (IllegalArgumentException ignored) {
			// Expected
		}
	}
}
