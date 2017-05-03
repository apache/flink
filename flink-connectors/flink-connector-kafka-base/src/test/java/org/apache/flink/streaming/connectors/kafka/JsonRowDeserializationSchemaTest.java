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

package org.apache.flink.streaming.connectors.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.util.serialization.JsonRowDeserializationSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonRowDeserializationSchemaTest {

	/**
	 * Tests simple deserialization.
	 */
	@Test
	public void testDeserialization() throws Exception {
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
			Types.ROW(
				new String[] { "id", "name", "bytes" },
				new TypeInformation<?>[] { Types.LONG(), Types.STRING(), Types.PRIMITIVE_ARRAY(Types.BYTE()) }
			)
		);

		Row deserialized = deserializationSchema.deserialize(serializedJson);

		assertEquals(3, deserialized.getArity());
		assertEquals(id, deserialized.getField(0));
		assertEquals(name, deserialized.getField(1));
		assertArrayEquals(bytes, (byte[]) deserialized.getField(2));
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
			Types.ROW(
				new String[] { "name" },
				new TypeInformation<?>[] { Types.STRING() }
			)
		);

		Row row = deserializationSchema.deserialize(serializedJson);

		assertEquals(1, row.getArity());
		assertNull("Missing field not null", row.getField(0));

		deserializationSchema.setFailOnMissingField(true);

		try {
			deserializationSchema.deserialize(serializedJson);
			fail("Did not throw expected Exception");
		} catch (IOException e) {
			assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}

	/**
	 * Tests that number of field names and types has to match.
	 */
	@Test
	public void testNumberOfFieldNamesAndTypesMismatch() throws Exception {
		try {
			new JsonRowDeserializationSchema(
				Types.ROW(
					new String[] { "one", "two", "three" },
					new TypeInformation<?>[] { Types.LONG() }
				)
			);
			fail("Did not throw expected Exception");
		} catch (IllegalArgumentException ignored) {
			// Expected
		}
	}
}
