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

package org.apache.flink.runtime.rest.messages.json;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import static org.junit.Assert.assertEquals;

/**
 * The test of {@link ExecutionAttemptIDSerializer} and {@link ExecutionAttemptIDDeserializer}.
 */
public class ExecutionAttemptIDSerializerDeserializerTest {
	@Test
	public void testSerializeAndDeserialize() throws IOException {
		final ObjectMapper mapper = new ObjectMapper();

		final Writer jsonWriter = new StringWriter();
		final JsonGenerator jsonGenerator = new JsonFactory().createGenerator(jsonWriter);
		final SerializerProvider serializerProvider = mapper.getSerializerProvider();

		final ExecutionAttemptIDSerializer serializer = new ExecutionAttemptIDSerializer();
		final ExecutionAttemptID expectedExecutionAttemptID = new ExecutionAttemptID();
		serializer.serialize(expectedExecutionAttemptID, jsonGenerator, serializerProvider);
		jsonGenerator.flush();

		final Reader jsonReader = new StringReader(jsonWriter.toString());
		final JsonParser parser = mapper.getFactory().createParser(jsonReader);
		parser.nextToken();
		final DeserializationContext ctxt = mapper.getDeserializationContext();
		final ExecutionAttemptIDDeserializer deserializer = new ExecutionAttemptIDDeserializer();

		final ExecutionAttemptID executionAttemptID = deserializer.deserialize(parser, ctxt);

		assertEquals(expectedExecutionAttemptID, executionAttemptID);
	}
}
