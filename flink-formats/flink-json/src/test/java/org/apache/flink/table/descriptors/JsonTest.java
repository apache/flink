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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link Json} descriptor.
 */
public class JsonTest extends DescriptorTestBase {

	private static final String JSON_SCHEMA =
		"{" +
		"    'title': 'Person'," +
		"    'type': 'object'," +
		"    'properties': {" +
		"        'firstName': {" +
		"            'type': 'string'" +
		"        }," +
		"        'lastName': {" +
		"            'type': 'string'" +
		"        }," +
		"        'age': {" +
		"            'description': 'Age in years'," +
		"            'type': 'integer'," +
		"            'minimum': 0" +
		"        }" +
		"    }," +
		"    'required': ['firstName', 'lastName']" +
		"}";

	@Test(expected = ValidationException.class)
	public void testInvalidMissingField() {
		addPropertyAndVerify(descriptors().get(0), "format.fail-on-missing-field", "DDD");
	}

	@Test(expected = ValidationException.class)
	public void testDuplicateSchema() {
		// we add an additional non-json schema
		addPropertyAndVerify(descriptors().get(0), "format.schema", "DDD");
	}

	@Test(expected = ValidationException.class)
	public void testInvalidIgnoreParseErrors() {
		addPropertyAndVerify(descriptors().get(0), "format.ignore-parse-errors", "DDD");
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public List<Descriptor> descriptors() {
		final Descriptor desc1 = new Json().jsonSchema("test");

		final Descriptor desc2 = new Json().jsonSchema(JSON_SCHEMA).failOnMissingField(true);

		final Descriptor desc3 = new Json()
			.schema(
				Types.ROW(
					new String[]{"test1", "test2"},
					new TypeInformation[]{Types.STRING(), Types.SQL_TIMESTAMP()}))
			.failOnMissingField(true);

		final Descriptor desc4 = new Json().deriveSchema();

		final Descriptor desc5 = new Json().failOnMissingField(false);

		final Descriptor desc6 = new Json().jsonSchema(JSON_SCHEMA).ignoreParseErrors(false);

		final Descriptor desc7 = new Json().ignoreParseErrors(true);

		return Arrays.asList(desc1, desc2, desc3, desc4, desc5, desc6, desc7);
	}

	@Override
	public List<Map<String, String>> properties() {
		final Map<String, String> props1 = new HashMap<>();
		props1.put("format.type", "json");
		props1.put("format.property-version", "1");
		props1.put("format.json-schema", "test");

		final Map<String, String> props2 = new HashMap<>();
		props2.put("format.type", "json");
		props2.put("format.property-version", "1");
		props2.put("format.json-schema", JSON_SCHEMA);
		props2.put("format.fail-on-missing-field", "true");

		final Map<String, String> props3 = new HashMap<>();
		props3.put("format.type", "json");
		props3.put("format.property-version", "1");
		props3.put("format.schema", "ROW<test1 VARCHAR, test2 TIMESTAMP>");
		props3.put("format.fail-on-missing-field", "true");

		final Map<String, String> props4 = new HashMap<>();
		props4.put("format.type", "json");
		props4.put("format.property-version", "1");
		props4.put("format.derive-schema", "true");

		final Map<String, String> props5 = new HashMap<>();
		props5.put("format.type", "json");
		props5.put("format.property-version", "1");
		props5.put("format.fail-on-missing-field", "false");

		final Map<String, String> props6 = new HashMap<>();
		props6.put("format.type", "json");
		props6.put("format.property-version", "1");
		props6.put("format.json-schema", JSON_SCHEMA);
		props6.put("format.ignore-parse-errors", "false");

		final Map<String, String> props7 = new HashMap<>();
		props7.put("format.type", "json");
		props7.put("format.property-version", "1");
		props7.put("format.ignore-parse-errors", "true");

		return Arrays.asList(props1, props2, props3, props4, props5, props6, props7);
	}

	@Override
	public DescriptorValidator validator() {
		return new JsonValidator();
	}
}
