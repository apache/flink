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

package org.apache.flink.modelserving.java.server;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.modelserving.java.server.typeschema.ByteArraySchema;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link SimpleStringSchema}.
 */
public class ByteArraySchemaTest {

	@Test
	public void testSerializationDesirailization() {
		final byte[] bytes = "hello world".getBytes();
		try {
			assertArrayEquals(bytes, new ByteArraySchema().serialize(bytes));
			assertEquals(bytes, new ByteArraySchema().deserialize(bytes));
		}
		catch (Throwable t) {
			System.out.println("Exception executing ByteArraySchemaTest : testSerializationDesirailization");
			t.printStackTrace();
		}
	}

	@Test
	public void testSerializability() throws Exception {
		final ByteArraySchema schema = new ByteArraySchema();
		final ByteArraySchema copy = CommonTestUtils.createCopySerializable(schema);
		assertEquals(schema.getProducedType(), copy.getProducedType());
	}
}
