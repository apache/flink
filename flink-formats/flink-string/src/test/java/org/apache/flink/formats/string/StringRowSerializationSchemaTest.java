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

package org.apache.flink.formats.string;

import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for the {@link StringRowSerializationSchema}.
 */
public class StringRowSerializationSchemaTest {

	@Test
	public void testStringSerialization() throws IOException {

		String data = "Hello World";
		byte[] bytes = data.getBytes("UTF-8");

		Row row = Row.of(data);
		StringRowSerializationSchema serializationSchema = new StringRowSerializationSchema("UTF-8");

		byte[] serializeData = serializationSchema.serialize(row);

		Assert.assertArrayEquals(bytes, serializeData);
	}

	@Test(expected = AssertionError.class)
	public void testStringSerializationWithWrongEncoding() throws IOException {

		String data = "你好世界";
		byte[] bytes = data.getBytes("GBK");

		Row row = Row.of(data);
		StringRowSerializationSchema serializationSchema = new StringRowSerializationSchema("UTF-8");

		byte[] serializeData = serializationSchema.serialize(row);

		Assert.assertArrayEquals(bytes, serializeData);
	}

	@Test(expected = RuntimeException.class)
	public void testStringSerializationWithUnsupportedEncoding() throws IOException {

		String data = "你好世界";
		byte[] bytes = data.getBytes("GBK");

		Row row = Row.of(data);
		StringRowSerializationSchema serializationSchema = new StringRowSerializationSchema("ddd");

		byte[] serializeData = serializationSchema.serialize(row);

		Assert.assertArrayEquals(bytes, serializeData);
	}

}
