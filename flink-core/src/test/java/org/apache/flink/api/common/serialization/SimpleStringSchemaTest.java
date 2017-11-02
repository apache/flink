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

package org.apache.flink.api.common.serialization;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link SimpleStringSchema}.
 */
public class SimpleStringSchemaTest {

	@Test
	public void testSerializationWithAnotherCharset() {
		final Charset charset = StandardCharsets.UTF_16BE;
		final String string = "之掃描古籍版實乃姚鼐的";
		final byte[] bytes = string.getBytes(charset);

		assertArrayEquals(bytes, new SimpleStringSchema(charset).serialize(string));
		assertEquals(string, new SimpleStringSchema(charset).deserialize(bytes));
	}

	@Test
	public void testSerializability() throws Exception {
		final SimpleStringSchema schema = new SimpleStringSchema(StandardCharsets.UTF_16LE);
		final SimpleStringSchema copy = CommonTestUtils.createCopySerializable(schema);

		assertEquals(schema.getCharset(), copy.getCharset());
	}
}
