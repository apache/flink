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

package org.apache.flink.table.descriptors;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.LogicalTypeParserTest;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DescriptorProperties}.
 */
public class DescriptorPropertiesTest {

	private static final String ARRAY_KEY = "my-array";
	private static final String FIXED_INDEXED_PROPERTY_KEY = "my-fixed-indexed-property";
	private static final String PROPERTY_1_KEY = "property-1";
	private static final String PROPERTY_2_KEY = "property-2";

	@Test
	public void testEquals() {
		DescriptorProperties properties1 = new DescriptorProperties();
		properties1.putString("hello1", "12");
		properties1.putString("hello2", "13");
		properties1.putString("hello3", "14");

		DescriptorProperties properties2 = new DescriptorProperties();
		properties2.putString("hello1", "12");
		properties2.putString("hello2", "13");
		properties2.putString("hello3", "14");

		DescriptorProperties properties3 = new DescriptorProperties();
		properties3.putString("hello1", "12");
		properties3.putString("hello3", "14");
		properties3.putString("hello2", "13");

		assertEquals(properties1, properties2);

		assertEquals(properties1, properties3);
	}

	@Test
	public void testMissingArray() {
		DescriptorProperties properties = new DescriptorProperties();

		testArrayValidation(properties, 0, Integer.MAX_VALUE);
	}

	@Test
	public void testArrayValues() {
		DescriptorProperties properties = new DescriptorProperties();

		properties.putString(ARRAY_KEY + ".0", "12");
		properties.putString(ARRAY_KEY + ".1", "42");
		properties.putString(ARRAY_KEY + ".2", "66");

		testArrayValidation(properties, 1, Integer.MAX_VALUE);

		assertEquals(
			Arrays.asList(12, 42, 66),
			properties.getArray(ARRAY_KEY, properties::getInt));
	}

	@Test
	public void testArraySingleValue() {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putString(ARRAY_KEY, "12");

		testArrayValidation(properties, 1, Integer.MAX_VALUE);

		assertEquals(
			Collections.singletonList(12),
			properties.getArray(ARRAY_KEY, properties::getInt));
	}

	@Test(expected = ValidationException.class)
	public void testArrayInvalidValues() {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putString(ARRAY_KEY + ".0", "12");
		properties.putString(ARRAY_KEY + ".1", "66");
		properties.putString(ARRAY_KEY + ".2", "INVALID");

		testArrayValidation(properties, 1, Integer.MAX_VALUE);
	}

	@Test(expected = ValidationException.class)
	public void testArrayInvalidSingleValue() {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putString(ARRAY_KEY, "INVALID");

		testArrayValidation(properties, 1, Integer.MAX_VALUE);
	}

	@Test(expected = ValidationException.class)
	public void testInvalidMissingArray() {
		DescriptorProperties properties = new DescriptorProperties();

		testArrayValidation(properties, 1, Integer.MAX_VALUE);
	}

	@Test(expected = ValidationException.class)
	public void testInvalidFixedIndexedProperties() {
		DescriptorProperties property = new DescriptorProperties();
		List<List<String>> list = new ArrayList<>();
		list.add(Arrays.asList("1", "string"));
		list.add(Arrays.asList("INVALID", "string"));
		property.putIndexedFixedProperties(
			FIXED_INDEXED_PROPERTY_KEY,
			Arrays.asList(PROPERTY_1_KEY, PROPERTY_2_KEY),
			list);
		testFixedIndexedPropertiesValidation(property);
	}

	@Test
	public void testRemoveKeys() {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putString("hello1", "12");
		properties.putString("hello2", "13");
		properties.putString("hello3", "14");

		DescriptorProperties actual = properties.withoutKeys(Arrays.asList("hello1", "hello3"));

		DescriptorProperties expected = new DescriptorProperties();
		expected.putString("hello2", "13");

		assertEquals(expected, actual);
	}

	@Test
	public void testPrefixedMap() {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putString("hello1", "12");
		properties.putString("hello2", "13");
		properties.putString("hello3", "14");

		Map<String, String> actual = properties.asPrefixedMap("prefix.");

		DescriptorProperties expected = new DescriptorProperties();
		expected.putString("prefix.hello1", "12");
		expected.putString("prefix.hello2", "13");
		expected.putString("prefix.hello3", "14");

		assertEquals(expected.asMap(), actual);
	}

	@Test
	public void testTableSchema() {
		TableSchema schema = TableSchema.builder()
			.field("f0", DataTypes.BIGINT().notNull())
			.field("f1", DataTypes.ROW(
				DataTypes.FIELD("q1", DataTypes.STRING()),
				DataTypes.FIELD("q2", DataTypes.TIMESTAMP(9))))
			.field("f2", DataTypes.STRING().notNull())
			.field("f3", DataTypes.BIGINT().notNull(), "f0 + 1")
			.field("f4", DataTypes.DECIMAL(10, 3))
			.watermark(
				"f1.q2",
				"`f1`.`q2` - INTERVAL '5' SECOND",
				DataTypes.TIMESTAMP(3))
			.primaryKey("constraint1", new String[] {"f0", "f2"})
			.build();

		DescriptorProperties properties = new DescriptorProperties();
		properties.putTableSchema("schema", schema);
		Map<String, String> actual = properties.asMap();
		Map<String, String> expected = new HashMap<>();
		expected.put("schema.0.name", "f0");
		expected.put("schema.0.data-type", "BIGINT NOT NULL");
		expected.put("schema.1.name", "f1");
		expected.put("schema.1.data-type", "ROW<`q1` VARCHAR(2147483647), `q2` TIMESTAMP(9)>");
		expected.put("schema.2.name", "f2");
		expected.put("schema.2.data-type", "VARCHAR(2147483647) NOT NULL");
		expected.put("schema.3.name", "f3");
		expected.put("schema.3.data-type", "BIGINT NOT NULL");
		expected.put("schema.3.expr", "f0 + 1");
		expected.put("schema.4.name", "f4");
		expected.put("schema.4.data-type", "DECIMAL(10, 3)");
		expected.put("schema.watermark.0.rowtime", "f1.q2");
		expected.put("schema.watermark.0.strategy.expr", "`f1`.`q2` - INTERVAL '5' SECOND");
		expected.put("schema.watermark.0.strategy.data-type", "TIMESTAMP(3)");
		expected.put("schema.primary-key.name", "constraint1");
		expected.put("schema.primary-key.columns", "f0,f2");
		assertEquals(expected, actual);

		TableSchema restored = properties.getTableSchema("schema");
		assertEquals(schema, restored);
	}

	@Test
	public void testLegacyTableSchema() {
		DescriptorProperties properties = new DescriptorProperties();
		Map<String, String> map = new HashMap<>();
		map.put("schema.0.name", "f0");
		map.put("schema.0.type", "VARCHAR");
		map.put("schema.1.name", "f1");
		map.put("schema.1.type", "BIGINT");
		map.put("schema.2.name", "f2");
		map.put("schema.2.type", "DECIMAL");
		map.put("schema.3.name", "f3");
		map.put("schema.3.type", "TIMESTAMP");
		map.put("schema.4.name", "f4");
		map.put("schema.4.type", "MAP<TINYINT, SMALLINT>");
		map.put("schema.5.name", "f5");
		map.put("schema.5.type", "ANY<java.lang.Class>");
		map.put("schema.6.name", "f6");
		map.put("schema.6.type", "PRIMITIVE_ARRAY<DOUBLE>");
		map.put("schema.7.name", "f7");
		map.put("schema.7.type", "OBJECT_ARRAY<TIME>");
		map.put("schema.8.name", "f8");
		map.put("schema.8.type", "ROW<q1 VARCHAR, q2 DATE>");
		map.put("schema.9.name", "f9");
		map.put("schema.9.type", "POJO<org.apache.flink.table.types.LogicalTypeParserTest$MyPojo>");
		properties.putProperties(map);
		TableSchema restored = properties.getTableSchema("schema");

		TableSchema expected = TableSchema.builder()
			.field("f0", Types.STRING)
			.field("f1", Types.LONG)
			.field("f2", Types.BIG_DEC)
			.field("f3", Types.SQL_TIMESTAMP)
			.field("f4", Types.MAP(Types.BYTE, Types.SHORT))
			.field("f5", Types.GENERIC(Class.class))
			.field("f6", Types.PRIMITIVE_ARRAY(Types.DOUBLE))
			.field("f7", Types.OBJECT_ARRAY(Types.SQL_TIME))
			.field("f8", Types.ROW_NAMED(new String[]{"q1", "q2"}, Types.STRING, Types.SQL_DATE))
			.field("f9", Types.POJO(LogicalTypeParserTest.MyPojo.class))
			.build();

		assertEquals(expected, restored);
	}

	private void testArrayValidation(
		DescriptorProperties properties,
		int minLength,
		int maxLength) {
		Consumer<String> validator = key -> properties.validateInt(key, false);

		properties.validateArray(
			ARRAY_KEY,
			validator,
			minLength,
			maxLength);
	}

	private void testFixedIndexedPropertiesValidation(DescriptorProperties properties) {

		Map<String, Consumer<String>> validatorMap = new HashMap<>();

		// PROPERTY_1 should be Int
		Consumer<String> validator1 = key -> properties.validateInt(key, false);
		validatorMap.put(PROPERTY_1_KEY, validator1);
		// PROPERTY_2 should be String
		Consumer<String> validator2 = key -> properties.validateString(key, false);
		validatorMap.put(PROPERTY_2_KEY, validator2);

		properties.validateFixedIndexedProperties(
			FIXED_INDEXED_PROPERTY_KEY,
			false,
			validatorMap
		);
	}
}
