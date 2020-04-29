package org.apache.flink.formats.json;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JsonRowDataDeserializationSchema} and {@link JsonRowDataSerializationSchema}.
 */
public class JsonRowDataSerDeSchemaTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testSerDe() throws Exception {
		long id = 1238123899121L;
		String name = "asdlkjasjkdla998y1122";
		byte[] bytes = new byte[1024];
		ThreadLocalRandom.current().nextBytes(bytes);
		BigDecimal decimal = new BigDecimal("123.456789");
		Double[] doubles = new Double[]{1.1, 2.2, 3.3};
		LocalDate date = LocalDate.parse("1990-10-14");
		LocalTime time = LocalTime.parse("12:12:43");
		Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
		Timestamp timestamp9 = Timestamp.valueOf("1990-10-14 12:12:43.123456789");

		Map<String, Long> map = new HashMap<>();
		map.put("flink", 123L);

		Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
		Map<String, Integer> innerMap = new HashMap<>();
		innerMap.put("key", 234);
		nestedMap.put("inner_map", innerMap);

		ObjectMapper objectMapper = new ObjectMapper();
		ArrayNode doubleNode = objectMapper.createArrayNode().add(1.1D).add(2.2D).add(3.3D);

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("bool", true);
		root.put("id", id);
		root.put("name", name);
		root.put("bytes", bytes);
		root.put("decimal", decimal);
		root.set("doubles", doubleNode);
		root.put("date", "1990-10-14");
		root.put("time", "12:12:43Z");
		root.put("timestamp3", "1990-10-14T12:12:43.123Z");
		root.put("timestamp9", "1990-10-14T12:12:43.123456789Z");
		root.putObject("map").put("flink", 123);
		root.putObject("map2map").putObject("inner_map").put("key", 234);

		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("id", BIGINT()),
			FIELD("name", STRING()),
			FIELD("bytes", BYTES()),
			FIELD("decimal", DECIMAL(9, 6)),
			FIELD("doubles", ARRAY(DOUBLE())),
			FIELD("date", DATE()),
			FIELD("time", TIME(0)),
			FIELD("timestamp3", TIMESTAMP(3)),
			FIELD("timestamp9", TIMESTAMP(9)),
			FIELD("map", MAP(STRING(), BIGINT())),
			FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))));
		RowType schema = (RowType) dataType.getLogicalType();
		RowDataTypeInfo resultTypeInfo = new RowDataTypeInfo(schema);

		JsonRowDataDeserializationSchema deserializationSchema = JsonRowDataDeserializationSchema.builder()
			.schema(schema)
			.resultTypeInfo(resultTypeInfo)
			.build();

		Row expected = new Row(12);
		expected.setField(0, true);
		expected.setField(1, id);
		expected.setField(2, name);
		expected.setField(3, bytes);
		expected.setField(4, decimal);
		expected.setField(5, doubles);
		expected.setField(6, date);
		expected.setField(7, time);
		expected.setField(8, timestamp3.toLocalDateTime());
		expected.setField(9, timestamp9.toLocalDateTime());
		expected.setField(10, map);
		expected.setField(11, nestedMap);

		RowData rowData = deserializationSchema.deserialize(serializedJson);
		Row actual = convertToExternal(rowData, dataType);
		assertEquals(expected, actual);

		// test serialization
		JsonRowDataSerializationSchema serializationSchema = JsonRowDataSerializationSchema.builder()
			.schema(schema)
			.build();

		byte[] actualBytes = serializationSchema.serialize(rowData);
		assertEquals(new String(serializedJson), new String(actualBytes));
	}

	@Test
	public void testSerDeMultiRows() throws Exception {
		RowType rowType = (RowType) ROW(
			FIELD("f1", INT()),
			FIELD("f2", BOOLEAN()),
			FIELD("f3", STRING())
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = JsonRowDataDeserializationSchema.builder()
			.schema(rowType)
			.resultTypeInfo(new RowDataTypeInfo(rowType))
			.build();
		JsonRowDataSerializationSchema serializationSchema = JsonRowDataSerializationSchema.builder()
			.schema(rowType)
			.build();

		ObjectMapper objectMapper = new ObjectMapper();

		// the first row
		{
			ObjectNode root = objectMapper.createObjectNode();
			root.put("f1", 1);
			root.put("f2", true);
			root.put("f3", "str");
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
			byte[] serializedJson = objectMapper.writeValueAsBytes(root);
			RowData rowData = deserializationSchema.deserialize(serializedJson);
			byte[] actual = serializationSchema.serialize(rowData);
			assertEquals(new String(serializedJson), new String(actual));
		}
	}

	@Test
	public void testSerDeMultiRowsWithNullValues() throws Exception {
		String[] jsons = new String[] {
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\"}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\", \"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"}, " +
				"\"ids\":[1, 2, 3]}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\"}",
		};

		String[] expected = new String[] {
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":{\"id\":\"281708d0-4092-4c21-9233-931950b6eccf\"}," +
				"\"ids\":[1,2,3]}",
			"{\"svt\":\"2020-02-24T12:58:09.209+0800\",\"ops\":null,\"ids\":null}",
		};

		RowType rowType = (RowType) ROW(
			FIELD("svt", STRING()),
			FIELD("ops", ROW(FIELD("id", STRING()))),
			FIELD("ids", ARRAY(INT()))
		).getLogicalType();

		JsonRowDataDeserializationSchema deserializationSchema = JsonRowDataDeserializationSchema.builder()
			.schema(rowType)
			.resultTypeInfo(new RowDataTypeInfo(rowType))
			.build();
		JsonRowDataSerializationSchema serializationSchema = JsonRowDataSerializationSchema.builder()
			.schema(rowType)
			.build();

		for (int i = 0; i < jsons.length; i++) {
			String json = jsons[i];
			RowData row = deserializationSchema.deserialize(json.getBytes());
			String result = new String(serializationSchema.serialize(row));
			assertEquals(expected[i], result);
		}
	}

	@Test
	public void testDeserializationMissingNode() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", 123123123);
		byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		DataType dataType = ROW(FIELD("name", STRING()));
		RowType schema = (RowType) dataType.getLogicalType();

		// pass on missing field
		JsonRowDataDeserializationSchema deserializationSchema = JsonRowDataDeserializationSchema.builder()
			.schema(schema)
			.resultTypeInfo(new RowDataTypeInfo(schema))
			.build();

		Row expected = new Row(1);
		Row actual = convertToExternal(deserializationSchema.deserialize(serializedJson), dataType);
		assertEquals(expected, actual);

		// fail on missing field
		deserializationSchema = JsonRowDataDeserializationSchema.builder()
			.schema(schema)
			.resultTypeInfo(new RowDataTypeInfo(schema))
			.failOnMissingField()
			.build();

		thrown.expect(IOException.class);
		thrown.expectMessage("Failed to deserialize JSON '{\"id\":123123123}'");
		deserializationSchema.deserialize(serializedJson);

		// ignore on parse error
		deserializationSchema = JsonRowDataDeserializationSchema.builder()
			.schema(schema)
			.resultTypeInfo(new RowDataTypeInfo(schema))
			.ignoreParseErrors()
			.build();
		actual = convertToExternal(deserializationSchema.deserialize(serializedJson), dataType);
		assertEquals(expected, actual);

		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled");
		// failOnMissingField and ignoreParseErrors both enabled
		JsonRowDataDeserializationSchema.builder()
			.schema(schema)
			.resultTypeInfo(new RowDataTypeInfo(schema))
			.failOnMissingField()
			.ignoreParseErrors()
			.build();
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
		JsonRowDataDeserializationSchema ignoreErrorsSchema = JsonRowDataDeserializationSchema.builder()
			.schema(spec.rowType)
			.resultTypeInfo(new RowDataTypeInfo(spec.rowType))
			.ignoreParseErrors()
			.build();
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
		JsonRowDataDeserializationSchema failingSchema = JsonRowDataDeserializationSchema.builder()
			.schema(spec.rowType)
			.resultTypeInfo(new RowDataTypeInfo(spec.rowType))
			.build();

		thrown.expectMessage(spec.errorMessage);
		failingSchema.deserialize(spec.json.getBytes());
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
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"abc\"}'"),

		TestSpec
			.json("{\"id\":112.013}")
			.rowType(ROW(FIELD("id", BIGINT())))
			.expect(Row.of(112L)),

		TestSpec
			.json("{\"id\":\"long\"}")
			.rowType(ROW(FIELD("id", BIGINT())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"long\"}'"),

		TestSpec
			.json("{\"id\":\"112.013.123\"}")
			.rowType(ROW(FIELD("id", FLOAT())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"112.013.123\"}'"),

		TestSpec
			.json("{\"id\":\"112.013.123\"}")
			.rowType(ROW(FIELD("id", DOUBLE())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"112.013.123\"}'"),

		TestSpec
			.json("{\"id\":\"18:00:243\"}")
			.rowType(ROW(FIELD("id", TIME())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"18:00:243\"}'"),

		TestSpec
			.json("{\"id\":\"20191112\"}")
			.rowType(ROW(FIELD("id", DATE())))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"20191112\"}'"),

		TestSpec
			.json("{\"id\":\"2019-11-12 18:00:12\"}")
			.rowType(ROW(FIELD("id", TIMESTAMP(0))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"2019-11-12 18:00:12\"}'"),

		TestSpec
			.json("{\"id\":\"abc\"}")
			.rowType(ROW(FIELD("id", DECIMAL(10, 3))))
			.expectErrorMessage("Failed to deserialize JSON '{\"id\":\"abc\"}'"),

		TestSpec
			.json("{\"row\":{\"id\":\"abc\"}}")
			.rowType(ROW(FIELD("row", ROW(FIELD("id", BOOLEAN())))))
			.expect(Row.of(new Row(1)))
			.expectErrorMessage("Failed to deserialize JSON '{\"row\":{\"id\":\"abc\"}}'"),

		TestSpec
			.json("{\"array\":[123, \"abc\"]}")
			.rowType(ROW(FIELD("array", ARRAY(INT()))))
			.expect(Row.of((Object) new Integer[]{123, null}))
			.expectErrorMessage("Failed to deserialize JSON '{\"array\":[123, \"abc\"]}'"),

		TestSpec
			.json("{\"map\":{\"key1\":\"123\", \"key2\":\"abc\"}}")
			.rowType(ROW(FIELD("map", MAP(STRING(), INT()))))
			.expect(Row.of(createHashMap("key1", 123, "key2", null)))
			.expectErrorMessage("Failed to deserialize JSON '{\"map\":{\"key1\":\"123\", \"key2\":\"abc\"}}'")


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
	}
}
