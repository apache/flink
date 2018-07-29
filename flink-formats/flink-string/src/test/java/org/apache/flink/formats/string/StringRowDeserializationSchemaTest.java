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
 * Tests for the {@link StringRowDeserializationSchema}.
 */
public class StringRowDeserializationSchemaTest {

	@Test
	public void testStringDeserialization() throws IOException {

		String data = "Hello World";
		byte[] bytes = data.getBytes("UTF-8");

		StringRowDeserializationSchema  deserializationSchema = new StringRowDeserializationSchema(
			"msg",
			"utf-8"
		);

		Row deserialized = deserializationSchema.deserialize(bytes);

		Assert.assertEquals(1, deserialized.getArity());
		Assert.assertEquals(data, deserialized.getField(0));

	}

	@Test(expected = IOException.class)
	public void testNullStringDeserialization() throws IOException {

		StringRowDeserializationSchema  deserializationSchema = new StringRowDeserializationSchema(
			"msg",
			"utf-8"
		);
		deserializationSchema.setFailOnNull(true);

		Row deserialized = deserializationSchema.deserialize(null);
	}

	@Test(expected = IOException.class)
	public void testEmptyStringDeserialization() throws IOException {

		StringRowDeserializationSchema  deserializationSchema = new StringRowDeserializationSchema(
			"msg",
			"utf-8"
		);
		deserializationSchema.setFailOnEmpty(true);

		Row deserialized = deserializationSchema.deserialize(new byte[0]);
	}

	@Test
	public void testStringDeserializationWithWrongEncoding() throws IOException {

		String data = "你好世界";
		byte[] bytes = data.getBytes("GBK");

		StringRowDeserializationSchema  deserializationSchema = new StringRowDeserializationSchema(
			"msg",
			"UTF-8"
		);

		Row deserialized = deserializationSchema.deserialize(bytes);
		Assert.assertEquals(1, deserialized.getArity());
		Assert.assertNotEquals(data, deserialized.getField(0));
	}

	@Test(expected = IOException.class)
	public void testStringDeserializationWithUnsupportedEncoding() throws IOException {

		String data = "你好世界";
		byte[] bytes = data.getBytes("GBK");

		StringRowDeserializationSchema  deserializationSchema = new StringRowDeserializationSchema(
			"msg",
			"ddd"
		);

		Row deserialized = deserializationSchema.deserialize(bytes);
		Assert.assertEquals(1, deserialized.getArity());
		Assert.assertNotEquals(data, deserialized.getField(0));
	}
}
