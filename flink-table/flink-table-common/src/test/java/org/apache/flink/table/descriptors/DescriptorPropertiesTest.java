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

import org.apache.flink.table.api.ValidationException;

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
