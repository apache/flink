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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class JSONDeserializationSchemaTest {
	@Test
	public void testDeserialize() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode initialValue = mapper.createObjectNode();
		initialValue.put("key", 4).put("value", "world");
		byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

		JSONDeserializationSchema schema = new JSONDeserializationSchema();
		ObjectNode deserializedValue = schema.deserialize(null, serializedValue, "", 0, 0);

		Assert.assertEquals(4, deserializedValue.get("key").asInt());
		Assert.assertEquals("world", deserializedValue.get("value").asText());
	}
}
