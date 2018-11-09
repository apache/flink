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
import org.apache.flink.util.FileUtils;

import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JsonRowSchemaConverter}.
 */
public class JsonRowSchemaConverterTest {

	@Test
	public void testComplexSchema() throws Exception {
		final URL url = getClass().getClassLoader().getResource("complex-schema.json");
		Objects.requireNonNull(url);
		final String schema = FileUtils.readFileUtf8(new File(url.getFile()));
		final TypeInformation<?> result = JsonRowSchemaConverter.convert(schema);

		final TypeInformation<?> expected = Types.ROW_NAMED(
			new String[] {"fn", "familyName", "additionalName", "tuples", "honorificPrefix", "url",
				"email", "tel", "sound", "org"},
			Types.STRING, Types.STRING, Types.BOOLEAN, Types.ROW(Types.BIG_DEC, Types.STRING, Types.STRING, Types.STRING),
			Types.OBJECT_ARRAY(Types.STRING), Types.STRING, Types.ROW_NAMED(new String[] {"type", "value"}, Types.STRING, Types.STRING),
			Types.ROW_NAMED(new String[] {"type", "value"}, Types.BIG_DEC, Types.STRING), Types.VOID,
			Types.ROW_NAMED(new String[] {"organizationUnit"}, Types.ROW()));

		assertEquals(expected, result);
	}

	@Test
	public void testReferenceSchema() throws Exception {
		final URL url = getClass().getClassLoader().getResource("reference-schema.json");
		Objects.requireNonNull(url);
		final String schema = FileUtils.readFileUtf8(new File(url.getFile()));
		final TypeInformation<?> result = JsonRowSchemaConverter.convert(schema);

		final TypeInformation<?> expected = Types.ROW_NAMED(
			new String[] {"billing_address", "shipping_address", "optional_address"},
			Types.ROW_NAMED(new String[] {"street_address", "city", "state"}, Types.STRING, Types.STRING, Types.STRING),
			Types.ROW_NAMED(new String[] {"street_address", "city", "state"}, Types.STRING, Types.STRING, Types.STRING),
			Types.ROW_NAMED(new String[] {"street_address", "city", "state"}, Types.STRING, Types.STRING, Types.STRING));

		assertEquals(expected, result);
	}

	@Test
	public void testAtomicType() {
		final TypeInformation<?> result = JsonRowSchemaConverter.convert("{ type: 'number' }");

		assertEquals(Types.BIG_DEC, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMissingType() {
		JsonRowSchemaConverter.convert("{ }");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongType() {
		JsonRowSchemaConverter.convert("{ type: 'whatever' }");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testArrayWithAdditionalItems() {
		JsonRowSchemaConverter.convert("{ type: 'array', items: [{type: 'integer'}], additionalItems: true }");
	}

	@Test
	public void testMissingProperties() {
		final TypeInformation<?> result = JsonRowSchemaConverter.convert("{ type: 'object' }");

		assertEquals(Types.ROW(), result);
	}

	@Test
	public void testNullUnionTypes() {
		final TypeInformation<?> result = JsonRowSchemaConverter.convert("{ type: ['string', 'null'] }");

		assertEquals(Types.STRING, result);
	}

	@Test
	public void testTimestamp() {
		final TypeInformation<?> result = JsonRowSchemaConverter.convert("{ type: 'string', format: 'date-time' }");

		assertEquals(Types.SQL_TIMESTAMP, result);
	}
}
