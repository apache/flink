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
package org.apache.flink.streaming.util.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;


/**
 * DeserializationSchema that deserializes a JSON String into an ObjectNode.
 * <p>
 * Fields can be accessed by calling objectNode.get(&lt;name>).as(&lt;type>)
 */
public class JSONDeserializationSchema extends AbstractDeserializationSchema<ObjectNode> {
	private ObjectMapper mapper;

	@Override
	public ObjectNode deserialize(byte[] message) throws IOException {
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
		return mapper.readValue(message, ObjectNode.class);
	}

	@Override
	public boolean isEndOfStream(ObjectNode nextElement) {
		return false;
	}

}
