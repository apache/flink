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
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link Csv} descriptor.
 */
public class CsvTest extends DescriptorTestBase {

	private static final TypeInformation<Row> SCHEMA = Types.ROW(
		new String[]{"a", "b", "c"},
		new TypeInformation[]{Types.STRING(), Types.INT(), Types.ROW(
				new String[]{"a", "b", "c"},
				new TypeInformation[]{Types.STRING(), Types.INT(), Types.BOOLEAN()}
		)}
	);

	private static final Descriptor CUSTOM_DESCRIPTOR_WITH_SCHEMA = new Csv()
		.schema(SCHEMA)
		.fieldDelimiter(';')
		.lineDelimiter("\r\n")
		.quoteCharacter('\'')
		.allowComments()
		.ignoreParseErrors()
		.arrayElementDelimiter("|")
		.escapeCharacter('\\')
		.nullLiteral("n/a");

	private static final Descriptor MINIMAL_DESCRIPTOR_WITH_DERIVED_SCHEMA = new Csv()
		.deriveSchema();

	@Test(expected = ValidationException.class)
	public void testInvalidAllowComments() {
		addPropertyAndVerify(CUSTOM_DESCRIPTOR_WITH_SCHEMA, "format.allow-comments", "DDD");
	}

	@Test(expected = ValidationException.class)
	public void testMissingSchema() {
		removePropertyAndVerify(CUSTOM_DESCRIPTOR_WITH_SCHEMA, "format.schema");
	}

	@Test(expected = ValidationException.class)
	public void testDuplicateSchema() {
		// we add an additional schema
		addPropertyAndVerify(
			MINIMAL_DESCRIPTOR_WITH_DERIVED_SCHEMA,
			"format.schema",
			"ROW<a VARCHAR, b INT, c ROW<a VARCHAR, b INT, c BOOLEAN>>");
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public List<Descriptor> descriptors() {
		return Arrays.asList(CUSTOM_DESCRIPTOR_WITH_SCHEMA, MINIMAL_DESCRIPTOR_WITH_DERIVED_SCHEMA);
	}

	@Override
	public List<Map<String, String>> properties() {
		final Map<String, String> props1 = new HashMap<>();
		props1.put("format.type", "csv");
		props1.put("format.property-version", "1");
		props1.put("format.schema", "ROW<a VARCHAR, b INT, c ROW<a VARCHAR, b INT, c BOOLEAN>>");
		props1.put("format.field-delimiter", ";");
		props1.put("format.line-delimiter", "\r\n");
		props1.put("format.quote-character", "'");
		props1.put("format.allow-comments", "true");
		props1.put("format.ignore-parse-errors", "true");
		props1.put("format.array-element-delimiter", "|");
		props1.put("format.escape-character", "\\");
		props1.put("format.null-literal", "n/a");

		final Map<String, String> props2 = new HashMap<>();
		props2.put("format.type", "csv");
		props2.put("format.property-version", "1");
		props2.put("format.derive-schema", "true");

		return Arrays.asList(props1, props2);
	}

	@Override
	public DescriptorValidator validator() {
		return new CsvValidator();
	}
}
