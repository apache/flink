/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for the{@link JSONKeyValueDeserializationSchema}.
 */
public class JSONKeyValueDeserializationSchemaTest {

	private static class TestConsumerRecord implements KeyedDeserializationSchema.Record {
		private final byte[] key;
		private final byte[] value;
		private final String topic;
		private final int partition;
		private final long offset;

		private TestConsumerRecord(byte[] key, byte[] value){
			this(key, value, "", 0, 0);
		}

		private TestConsumerRecord(byte[] key, byte[] value, String topic, int partition, long offset) {
			this.key = key;
			this.value = value;
			this.topic = topic;
			this.partition = partition;
			this.offset = offset;
		}

		@Override
		public byte[] key() {
			return key;
		}

		@Override
		public byte[] value() {
			return value;
		}

		@Override
		public String topic() {
			return topic;
		}

		@Override
		public int partition() {
			return partition;
		}

		@Override
		public long offset() {
			return offset;
		}
	}

	@Test
	public void testDeserializeWithoutMetadata() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode initialKey = mapper.createObjectNode();
		initialKey.put("index", 4);
		byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

		ObjectNode initialValue = mapper.createObjectNode();
		initialValue.put("word", "world");
		byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

		JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
		ObjectNode deserializedValue = schema.deserialize(new TestConsumerRecord(serializedKey, serializedValue));

		Assert.assertTrue(deserializedValue.get("metadata") == null);
		Assert.assertEquals(4, deserializedValue.get("key").get("index").asInt());
		Assert.assertEquals("world", deserializedValue.get("value").get("word").asText());
	}

	@Test
	public void testDeserializeWithoutKey() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		byte[] serializedKey = null;

		ObjectNode initialValue = mapper.createObjectNode();
		initialValue.put("word", "world");
		byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

		JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
		ObjectNode deserializedValue = schema.deserialize(new TestConsumerRecord(serializedKey, serializedValue));

		Assert.assertTrue(deserializedValue.get("metadata") == null);
		Assert.assertTrue(deserializedValue.get("key") == null);
		Assert.assertEquals("world", deserializedValue.get("value").get("word").asText());
	}

	@Test
	public void testDeserializeWithoutValue() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode initialKey = mapper.createObjectNode();
		initialKey.put("index", 4);
		byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

		byte[] serializedValue = null;

		JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
		ObjectNode deserializedValue = schema.deserialize(new TestConsumerRecord(serializedKey, serializedValue));

		Assert.assertTrue(deserializedValue.get("metadata") == null);
		Assert.assertEquals(4, deserializedValue.get("key").get("index").asInt());
		Assert.assertTrue(deserializedValue.get("value") == null);
	}

	@Test
	public void testDeserializeWithMetadata() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode initialKey = mapper.createObjectNode();
		initialKey.put("index", 4);
		byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

		ObjectNode initialValue = mapper.createObjectNode();
		initialValue.put("word", "world");
		byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

		JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(true);
		ObjectNode deserializedValue = schema.deserialize(
			new TestConsumerRecord(
				serializedKey, serializedValue, "topic#1", 3, 4));

		Assert.assertEquals(4, deserializedValue.get("key").get("index").asInt());
		Assert.assertEquals("world", deserializedValue.get("value").get("word").asText());
		Assert.assertEquals("topic#1", deserializedValue.get("metadata").get("topic").asText());
		Assert.assertEquals(4, deserializedValue.get("metadata").get("offset").asInt());
		Assert.assertEquals(3, deserializedValue.get("metadata").get("partition").asInt());
	}
}
