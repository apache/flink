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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link StringFormat} descriptor.
 */
public class StringFormatTest extends DescriptorTestBase {

	@Test(expected = ValidationException.class)
	public void testInvalidFailOnNull() {
		addPropertyAndVerify(descriptors().get(0), "format.fail-on-null", "DDD");
	}

	@Test(expected = ValidationException.class)
	public void testInvalidFailOnEmpty() {
		addPropertyAndVerify(descriptors().get(0), "format.fail-on-empty", "DDD");
	}

	@Test(expected = ValidationException.class)
	public void testMissingSchema() {
		addPropertyAndVerify(descriptors().get(0), "format.schema", "");
	}

	@Override
	public List<Descriptor> descriptors() {
		Descriptor desc1 = new StringFormat().setSchema("nid");
		Descriptor desc2 = new StringFormat().setSchema("nid").setEncoding("UTF-8");
		Descriptor desc3 = new StringFormat().setSchema("nid").setFailOnNull(false);
		Descriptor desc4 = new StringFormat().setSchema("nid").setFailOnEmpty(true);

		return Arrays.asList(desc1, desc2, desc3, desc4);
	}

	@Override
	public List<Map<String, String>> properties() {
		final Map<String, String> prop1 = new HashMap<>();
		prop1.put("format.type", "string");
		prop1.put("format.property-version", "1");
		prop1.put("format.schema", "nid");

		final Map<String, String> prop2 = new HashMap<>();
		prop2.put("format.type", "string");
		prop2.put("format.property-version", "1");
		prop2.put("format.schema", "nid");
		prop2.put("format.encoding", "UTF-8");

		final Map<String, String> prop3 = new HashMap<>();
		prop3.put("format.type", "string");
		prop3.put("format.property-version", "1");
		prop3.put("format.schema", "nid");
		prop3.put("format.fail-on-null", "false");

		final Map<String, String> prop4 = new HashMap<>();
		prop4.put("format.type", "string");
		prop4.put("format.property-version", "1");
		prop4.put("format.schema", "nid");
		prop4.put("format.fail-on-empty", "true");

		return Arrays.asList(prop1, prop2, prop3, prop4);
	}

	@Override
	public DescriptorValidator validator() {
		return new StringValidator();
	}
}
