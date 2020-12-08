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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JsonRowDataDeserializationSchema} and {@link JsonRowDataSerializationSchema}.
 */
public class JsonRowDataSerDeSchemaTest {

	@Test
	public void testSerDe() throws Exception {
		byte tinyint = 'c';
		short smallint = 128;
		int intValue = 45536;
		float floatValue = 33.333F;
		long bigint = 1238123899121L;
		String name = "asdlkjasjkdla998y1122";
		byte[] bytes = new byte[1024];
		ThreadLocalRandom.current().nextBytes(bytes);
		BigDecimal decimal = new BigDecimal("123.456789");
		Double[] doubles = new Double[]{1.1, 2.2, 3.3};
		LocalDate date = LocalDate.parse("1990-10-14");
		LocalTime time = LocalTime.parse("12:12:43");
		Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
		Timestamp timestamp9 = Timestamp.valueOf("1990-10-14 12:12:43.123456789");
		Instant timestampWithLocalZone =
			LocalDateTime.of(1990, 10, 14, 12, 12, 43, 123456789).
				atOffset(ZoneOffset.of("Z")).toInstant();

		Map<String, Long> map = new HashMap<>();
		map.put("flink", 123L);

		Map<String, Integer> multiSet = new HashMap<>();
		multiSet.put("blink", 2);

		Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
		Map<String, Integer> innerMap = new HashMap<>();
		innerMap.put("key", 234);
		nestedMap.put("inner_map", innerMap);

		ObjectMapper objectMapper = new ObjectMapper();
		ArrayNode doubleNode = objectMapper.createArrayNode().add(1.1D).add(2.2D).add(3.3D);

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("bool", true);
		root.put("tinyint", tinyint);
		root.put("smallint", smallint);
		root.put("int", intValue);
		root.put("bigint", bigint);
		root.put("float", floatValue);
		root.put("name", name);
		root.put("bytes", bytes);
		root.put("decimal", decimal);
		root.set("doubles", doubleNode);
		root.put("date", "1990-10-14");
		root.put("time", "12:12:43");
		root.put("timestamp3", "1990-10-14T12:12:43.123");
		root.put("timestamp9", "1990-10-14T12:12:43.123456789");
		root.put("timestampWithLocalZone", "1990-10-14T12:12:43.123456789Z");
		root.putObject("map").put("flink", 123);
		root.putObject("multiSet").put("blink", 2);
		root.putObject("map2map").putObject("inner_map").put("key", 234);

		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("tinyint", TINYINT()),
			FIELD("smallint", SMALLINT()),
			FIELD("int", INT()),
			FIELD("bigint", BIGINT()),
			FIELD("float", FLOAT()),
			FIELD("name", STRING()),
			FIELD("bytes", BYTES()),
			FIELD("decimal", DECIMAL(9, 6)),
			FIELD("doubles", ARRAY(DOUBLE())),
			FIELD("date", DATE()),
			FIELD("time", TIME(0)),
			FIELD("timestamp3", TIMESTAMP(3)),
			FIELD("timestamp9", TIMESTAMP(9)),
			FIELD("timestampWithLocalZone", TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
			FIELD("map", MAP(STRING(), BIGINT())),
			FIELD("multiSet", MULTISET(STRING())),
			FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))));
		RowType schema = (RowType) dataType.getLogicalType();
		TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, resultTypeInfo, false, false, TimestampFormat.ISO_8601);

		Row expected = new Row(18);
		expected.setField(0, true);
		expected.setField(1, tinyint);
		expected.setField(2, smallint);
		expected.setField(3, intValue);
		expected.setField(4, bigint);
		expected.setField(5, floatValue);
		expected.setField(6, name);
		expected.setField(7, bytes);
		expected.setField(8, decimal);
		expected.setField(9, doubles);
		expected.setField(10, date);
		expected.setField(11, time);
		expected.setField(12, timestamp3.toLocalDateTime());
		expected.setField(13, timestamp9.toLocalDateTime());
		expected.setField(14, timestampWithLocalZone);
		expected.setField(15, map);
		expected.setField(16, multiSet);
		expected.setField(17, nestedMap);

		RowData rowData = deserializationSchema.deserialize(serializedJson);
		Row actual = convertToExternal(rowData, dataType);
		assertEquals(expected, actual);

		// test serialization
		JsonRowDataSerializationSchema serializationSchema =
			new JsonRowDataSerializationSchema(
				schema,
				TimestampFormat.ISO_8601,
				JsonOptions.MapNullKeyMode.LITERAL,
				"null");

		byte[] actualBytes = serializationSchema.serialize(rowData);
		assertEquals(new String(serializedJson), new String(actualBytes));
	}

	/**
	 * Tests the deserialization slow path,
	 * e.g. convert into string and use {@link Double#parseDouble(String)}.
	 */
	@Test
	public void testSlowDeserialization() throws Exception {
		Random random = new Random();
		boolean bool = random.nextBoolean();
		int integer = random.nextInt();
		long bigint = random.nextLong();
		double doubleValue = random.nextDouble();
		float floatValue = random.nextFloat();

		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode root = objectMapper.createObjectNode();
		root.put("bool", String.valueOf(bool));
		root.put("int", String.valueOf(integer));
		root.put("bigint", String.valueOf(bigint));
		root.put("double1", String.valueOf(doubleValue));
		root.put("double2", new BigDecimal(doubleValue));
		root.put("float1", String.valueOf(floatValue));
		root.put("float2", new BigDecimal(floatValue));

		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("int", INT()),
			FIELD("bigint", BIGINT()),
			FIELD("double1", DOUBLE()),
			FIELD("double2", DOUBLE()),
			FIELD("float1", FLOAT()),
			FIELD("float2", FLOAT())
		);
		RowType rowType = (RowType) dataType.getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, InternalTypeInfo.of(rowType), false, false,  TimestampFormat.ISO_8601);

		Row expected = new Row(7);
		expected.setField(0, bool);
		expected.setField(1, integer);
		expected.setField(2, bigint);
		expected.setField(3, doubleValue);
		expected.setField(4, doubleValue);
		expected.setField(5, floatValue);
		expected.setField(6, floatValue);

		RowData rowData = deserializationSchema.deserialize(serializedJson);
		Row actual = convertToExternal(rowData, dataType);
		assertEquals(expected, actual);
	}

	@Test
	public void testSerDeMultiRows() throws Exception {
		RowType rowType = (RowType) ROW(
			FIELD("f1", INT()),
			FIELD("f2", BOOLEAN()),
			FIELD("f3", STRING()),
			FIELD("f4", MAP(STRING(), STRING())),
			FIELD("f5", ARRAY(STRING())),
			FIELD("f6", ROW(
				FIELD("f1", STRING()),
				FIELD("f2", INT())))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, InternalTypeInfo.of(rowType), false, false,  TimestampFormat.ISO_8601);
		JsonRowDataSerializationSchema serializationSchema =
			new JsonRowDataSerializationSchema(
				rowType, TimestampFormat.ISO_8601, JsonOptions.MapNullKeyMode.LITERAL, "null");

		ObjectMapper objectMapper = new ObjectMapper();

		// the first row
		{
			ObjectNode root = objectMapper.createObjectNode();
			root.put("f1", 1);
			root.put("f2", true);
			root.put("f3", "str");
			ObjectNode map = root.putObject("f4");
			map.put("hello1", "flink");
			ArrayNode array = root.putArray("f5");
			array.add("element1");
			array.add("element2");
			ObjectNode row = root.putObject("f6");
			row.put("f1", "this is row1");
			row.put("f2", 12);
			byte[] serializedJson = objectMapper.writeValueAsBytes(root);
			RowData rowData = deserializationSchema.deserialize(serializedJson);
			byte[] actual = serializationSchema.serialize(rowData);
			assertEquals(new String(serializedJson), new String(actual));
		}

		// the second row
		{
			ObjectNode root = objectMapper.createObjectNode();
			root.put("f1", 10);
			root.put("f2", false);
			root.put("f3", "newStr");
			ObjectNode map = root.putObject("f4");
			map.put("hello2", "json");
			ArrayNode array = root.putArray("f5");
			array.add("element3");
			array.add("element4");
			ObjectNode row = root.putObject("f6");
			row.put("f1", "this is row2");
			row.putNull("f2");
			byte[] serializedJson = objectMapper.writeValueAsBytes(root);
			RowData rowData = deserializationSchema.deserialize(serializedJson);
			byte[] actual = serializationSchema.serialize(rowData);
			assertEquals(new String(serializedJson), new String(actual));
		}
	}

	@Test
	public void testSerDeMultiRowsWithNullValues() throws Exception {
		String[] jsons = new String[] {
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"metrics\":{\"k1\":10.01,\"k2\":\"invalid\"}}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\", \"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"}, " +
				"\"ids\":[1, 2, 3]}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"metrics\":{}}",
		};

		String[] expected = new String[] {
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null,\"metrics\":{\"k1\":10.01,\"k2\":null}}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"}," +
				"\"ids\":[1,2,3],\"metrics\":null}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null,\"metrics\":{}}",
		};

		RowType rowType = (RowType) ROW(
			FIELD("svt", STRING()),
			FIELD("ops", ROW(FIELD("id", STRING()))),
			FIELD("ids", ARRAY(INT())),
			FIELD("metrics", MAP(STRING(), DOUBLE()))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, InternalTypeInfo.of(rowType), false, true, TimestampFormat.ISO_8601);
		JsonRowDataSerializationSchema serializationSchema =
			new JsonRowDataSerializationSchema(
				rowType, TimestampFormat.ISO_8601, JsonOptions.MapNullKeyMode.LITERAL, "null");

		for (int i = 0; i < jsons.length; i++) {
			String json = jsons[i];
			RowData row = deserializationSchema.deserialize(json.getBytes());
			String result = new String(serializationSchema.serialize(row));
			assertEquals(expected[i], result);
		}
	}

	@Test
	public void testDeserializationMissingNode() throws Exception {
		DataType dataType = ROW(FIELD("name", STRING()));
		RowType schema = (RowType) dataType.getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, InternalTypeInfo.of(schema), true, false, TimestampFormat.ISO_8601);
		RowData rowData = deserializationSchema.deserialize("".getBytes());
		assertEquals(null, rowData);
	}

	@Test
	public void testDeserializationMissingField() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", 123123123);
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(FIELD("name", STRING()));
		RowType schema = (RowType) dataType.getLogicalType();

		// pass on missing field
		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, InternalTypeInfo.of(schema), false, false, TimestampFormat.ISO_8601);

		Row expected = new Row(1);
		Row actual = convertToExternal(deserializationSchema.deserialize(serializedJson), dataType);
		assertEquals(expected, actual);

		// fail on missing field
		deserializationSchema = deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, InternalTypeInfo.of(schema), true, false, TimestampFormat.ISO_8601);

		String errorMessage = "Failed to deserialize JSON '{\"id\":123123123}'.";
		try {
			deserializationSchema.deserialize(serializedJson);
			fail("expecting exception message: " + errorMessage);
		} catch (Throwable t) {
			assertEquals(errorMessage, t.getMessage());
		}

		// ignore on parse error
		deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, InternalTypeInfo.of(schema), false, true, TimestampFormat.ISO_8601);
		actual = convertToExternal(deserializationSchema.deserialize(serializedJson), dataType);
		assertEquals(expected, actual);

		errorMessage = "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.";
		try {
			// failOnMissingField and ignoreParseErrors both enabled
			new JsonRowDataDeserializationSchema(
				schema, InternalTypeInfo.of(schema), true, true, TimestampFormat.ISO_8601);
			Assert.fail("expecting exception message: " + errorMessage);
		} catch (Throwable t) {
			assertEquals(errorMessage, t.getMessage());
		}
	}

	@Test
	public void testSerDeSQLTimestampFormat() throws Exception {
		RowType rowType = (RowType) ROW(
			FIELD("timestamp3", TIMESTAMP(3)),
			FIELD("timestamp9", TIMESTAMP(9)),
			FIELD("timestamp_with_local_timezone3", TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
			FIELD("timestamp_with_local_timezone9", TIMESTAMP_WITH_LOCAL_TIME_ZONE(9))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			rowType, InternalTypeInfo.of(rowType), false, false, TimestampFormat.SQL);
		JsonRowDataSerializationSchema serializationSchema =
			new JsonRowDataSerializationSchema(
					rowType, TimestampFormat.SQL, JsonOptions.MapNullKeyMode.LITERAL, "null");

		ObjectMapper objectMapper = new ObjectMapper();

		ObjectNode root = objectMapper.createObjectNode();
		root.put("timestamp3", "1990-10-14 12:12:43.123");
		root.put("timestamp9", "1990-10-14 12:12:43.123456789");
		root.put("timestamp_with_local_timezone3", "1990-10-14 12:12:43.123Z");
		root.put("timestamp_with_local_timezone9", "1990-10-14 12:12:43.123456789Z");
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);
		RowData rowData = deserializationSchema.deserialize(serializedJson);
		byte[] actual = serializationSchema.serialize(rowData);
		assertEquals(new String(serializedJson), new String(actual));
	}

	@Test
	public void testSerializationMapNullKey() throws Exception {
		RowType rowType = (RowType) ROW(
			FIELD("nestedMap", MAP(STRING(), MAP(STRING(), INT())))
		).getLogicalType();

		// test data
		// use LinkedHashMap to make sure entries order
		Map<StringData, Integer> map = new LinkedHashMap<>();
		map.put(StringData.fromString("no-null key"), 1);
		map.put(StringData.fromString(null), 2);
		GenericMapData mapData = new GenericMapData(map);

		Map<StringData, GenericMapData> nestedMap = new LinkedHashMap<>();
		nestedMap.put(StringData.fromString("no-null key"), mapData);
		nestedMap.put(StringData.fromString(null), mapData);

		GenericMapData nestedMapData = new GenericMapData(nestedMap);
		GenericRowData rowData = new GenericRowData(1);
		rowData.setField(0, nestedMapData);

		JsonRowDataSerializationSchema serializationSchema1 =
			new JsonRowDataSerializationSchema(
				rowType, TimestampFormat.SQL, JsonOptions.MapNullKeyMode.FAIL, "null");
		// expect message for serializationSchema1
		String errorMessage1 = "JSON format doesn't support to serialize map data with null keys."
			+ " You can drop null key entries or encode null in literals by specifying map-null-key.mode option.";

		JsonRowDataSerializationSchema serializationSchema2 =
			new JsonRowDataSerializationSchema(
				rowType, TimestampFormat.SQL, JsonOptions.MapNullKeyMode.DROP, "null");
		// expect result for serializationSchema2
		String expectResult2 = "{\"nestedMap\":{\"no-null key\":{\"no-null key\":1}}}";

		JsonRowDataSerializationSchema serializationSchema3 =
			new JsonRowDataSerializationSchema(
				rowType, TimestampFormat.SQL, JsonOptions.MapNullKeyMode.LITERAL, "nullKey");
		// expect result for serializationSchema3
		String expectResult3 =
			"{\"nestedMap\":{\"no-null key\":{\"no-null key\":1,\"nullKey\":2},\"nullKey\":{\"no-null key\":1,\"nullKey\":2}}}";

		try {
			// throw exception when mapNullKey Mode is fail
			serializationSchema1.serialize(rowData);
			Assert.fail("expecting exception message: " + errorMessage1);
		} catch (Throwable t) {
			assertEquals(errorMessage1, t.getCause().getMessage());
		}

		// mapNullKey Mode is drop
		byte[] actual2 = serializationSchema2.serialize(rowData);
		assertEquals(expectResult2, new String(actual2));

		// mapNullKey Mode is literal
		byte[] actual3 = serializationSchema3.serialize(rowData);
		assertEquals(expectResult3, new String(actual3));
	}

	@Test
	public void testJsonParse() throws Exception {
		for (TestSpec spec : testData) {
			testIgnoreParseErrors(spec);
			if (spec.errorMessage != null) {
				testParseErrors(spec);
			}
		}
	}

	private void testIgnoreParseErrors(TestSpec spec) throws Exception {
		// the parsing field should be null and no exception is thrown
		JsonRowDataDeserializationSchema ignoreErrorsSchema = new JsonRowDataDeserializationSchema(
			spec.rowType, InternalTypeInfo.of(spec.rowType), false, true,
			spec.timestampFormat);
		Row expected;
		if (spec.expected != null) {
			expected = spec.expected;
		} else {
			expected = new Row(1);
		}
		RowData rowData = ignoreErrorsSchema.deserialize(spec.json.getBytes());
		Row actual = convertToExternal(rowData, fromLogicalToDataType(spec.rowType));
		assertEquals("Test Ignore Parse Error: " + spec.json,
			expected,
			actual);
	}

	private void testParseErrors(TestSpec spec) throws Exception {
		// expect exception if parse error is not ignored
		JsonRowDataDeserializationSchema failingSchema = new JsonRowDataDeserializationSchema(
			spec.rowType, InternalTypeInfo.of(spec.rowType), false, false,
			spec.timestampFormat);

		try {
			failingSchema.deserialize(spec.json.getBytes());
			fail("expecting exception " + spec.errorMessage);
		} catch (Throwable t) {
			assertEquals(t.getMessage(), spec.errorMessage);
		}
	}

	private static List<TestSpec> testData = Arrays.asList(
		TestSpec
			.json("{\"id\": \"trueA\"}")
			.rowType(ROW(FIELD("id", BOOLEAN())))
			.expect(Row.of(false)),

		TestSpec
			.json("{\"id\": true}")
			.rowType(ROW(FIELD("id", BOOLEAN())))
			.expect(Row.of(true)),

		TestSpec
			.json("{\"id\":\"abc\"}")
			.rowType(ROW(FIELD("id", INT())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"abc\"}'."),

		TestSpec
			.json("{\"id\":112.013}")
			.rowType(ROW(FIELD("id", BIGINT())))
			.expect(Row.of(112L)),

		TestSpec
			.json("{\"id\":\"long\"}")
			.rowType(ROW(FIELD("id", BIGINT())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"long\"}'."),

		TestSpec
			.json("{\"id\":\"112.013.123\"}")
			.rowType(ROW(FIELD("id", FLOAT())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"112.013.123\"}'."),

		TestSpec
			.json("{\"id\":\"112.013.123\"}")
			.rowType(ROW(FIELD("id", DOUBLE())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"112.013.123\"}'."),

		TestSpec
			.json("{\"id\":\"18:00:243\"}")
			.rowType(ROW(FIELD("id", TIME())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"18:00:243\"}'."),

		TestSpec
			.json("{\"id\":\"18:00:243\"}")
			.rowType(ROW(FIELD("id", TIME())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"18:00:243\"}'."),

		TestSpec
			.json("{\"id\":\"20191112\"}")
			.rowType(ROW(FIELD("id", DATE())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"20191112\"}'."),

		TestSpec
			.json("{\"id\":\"20191112\"}")
			.rowType(ROW(FIELD("id", DATE())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"20191112\"}'."),

		TestSpec
			.json("{\"id\":true}")
			.rowType(ROW(FIELD("id", STRING())))
			.expect(Row.of("true")),

		TestSpec
			.json("{\"id\":123.234}")
			.rowType(ROW(FIELD("id", STRING())))
			.expect(Row.of("123.234")),

		TestSpec
			.json("{\"id\":1234567}")
			.rowType(ROW(FIELD("id", STRING())))
			.expect(Row.of("1234567")),

		TestSpec
			.json("{\"id\":\"string field\"}")
			.rowType(ROW(FIELD("id", STRING())))
			.expect(Row.of("string field")),

		TestSpec
			.json("{\"id\":[\"array data1\",\"array data2\",123,234.345]}")
			.rowType(ROW(FIELD("id", STRING())))
			.expect(Row.of("[\"array data1\",\"array data2\",123,234.345]")),

		TestSpec
			.json("{\"id\":{\"k1\":123,\"k2\":234.234,\"k3\":\"string data\"}}")
			.rowType(ROW(FIELD("id", STRING())))
			.expect(Row.of("{\"k1\":123,\"k2\":234.234,\"k3\":\"string data\"}")),

		TestSpec
			.json("{\"id\":\"2019-11-12 18:00:12\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.timestampFormat(TimestampFormat.ISO_8601)
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12 18:00:12\"}'."),

		TestSpec
			.json("{\"id\":\"2019-11-12T18:00:12\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12T18:00:12\"}'."),

		TestSpec
			.json("{\"id\":\"2019-11-12T18:00:12Z\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12T18:00:12Z\"}'."),

		TestSpec
			.json("{\"id\":\"2019-11-12T18:00:12Z\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.timestampFormat(TimestampFormat.ISO_8601)
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12T18:00:12Z\"}'."),

		TestSpec
			.json("{\"id\":\"abc\"}")
			.rowType(ROW(FIELD("id", DECIMAL(10, 3))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"abc\"}'."),

		TestSpec
			.json("{\"row\":{\"id\":\"abc\"}}")
			.rowType(ROW(FIELD("row", ROW(FIELD("id", BOOLEAN())))))
			.expect(Row.of(Row.of(false))),

		TestSpec
			.json("{\"array\":[123, \"abc\"]}")
			.rowType(ROW(FIELD("array", ARRAY(INT()))))
			.expect(Row.of((Object) new Integer[]{123, null}))
			.expectErrorMessage("Failed to deserialize JSON '{\"array\":[123, \"abc\"]}'."),

		TestSpec
			.json("{\"map\":{\"key1\":\"123\", \"key2\":\"abc\"}}")
			.rowType(ROW(FIELD("map", MAP(STRING(), INT()))))
			.expect(Row.of(createHashMap("key1", 123, "key2", null)))
			.expectErrorMessage("Failed to deserialize JSON '{\"map\":{\"key1\":\"123\", \"key2\":\"abc\"}}'."),

		TestSpec
			.json("{\"id\":\"2019-11-12T18:00:12\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP_WITH_LOCAL_TIME_ZONE(0))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12T18:00:12\"}'."),

		TestSpec
			.json("{\"id\":\"2019-11-12T18:00:12+0800\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP_WITH_LOCAL_TIME_ZONE(0))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12T18:00:12+0800\"}'."),

		TestSpec
			.json("{\"id\":1,\"factor\":799.929496989092949698}")
			.rowType(ROW(FIELD("id", INT()), FIELD("factor", DECIMAL(38, 18))))
			.expect(Row.of(1, new BigDecimal("799.929496989092949698")))
	);

	private static Map<String, Integer> createHashMap(String k1, Integer v1, String k2, Integer v2) {
		Map<String, Integer> map = new HashMap<>();
		map.put(k1, v1);
		map.put(k2, v2);
		return map;
	}

	@SuppressWarnings("unchecked")
	private static Row convertToExternal(RowData rowData, DataType dataType) {
		return (Row) DataFormatConverters.getConverterForDataType(dataType).toExternal(rowData);
	}

	private static class TestSpec {
		private final String json;
		private RowType rowType;
		private TimestampFormat timestampFormat = TimestampFormat.SQL;
		private Row expected;
		private String errorMessage;

		private TestSpec(String json) {
			this.json = json;
		}

		public static TestSpec json(String json) {
			return new TestSpec(json);
		}

		TestSpec expect(Row row) {
			this.expected = row;
			return this;
		}

		TestSpec rowType(DataType rowType) {
			this.rowType = (RowType) rowType.getLogicalType();
			return this;
		}

		TestSpec expectErrorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
			return this;
		}

		TestSpec timestampFormat(TimestampFormat timestampFormat){
			this.timestampFormat = timestampFormat;
			return this;
		}
	}
}
