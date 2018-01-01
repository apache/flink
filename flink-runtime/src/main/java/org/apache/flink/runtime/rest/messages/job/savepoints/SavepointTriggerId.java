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

package org.apache.flink.runtime.rest.messages.job.savepoints;

import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import javax.xml.bind.DatatypeConverter;

import java.io.IOException;

/**
 * Identifies a savepoint trigger request.
 */
@JsonSerialize(using = SavepointTriggerId.SavepointTriggerIdSerializer.class)
@JsonDeserialize(using = SavepointTriggerId.SavepointTriggerIdDeserializer.class)
public class SavepointTriggerId extends AbstractID {

	private static final long serialVersionUID = 1L;

	public SavepointTriggerId() {
	}

	private SavepointTriggerId(final byte[] bytes) {
		super(bytes);
	}

	public static SavepointTriggerId fromHexString(String hexString) {
		return new SavepointTriggerId(DatatypeConverter.parseHexBinary(hexString));
	}

	/**
	 * JSON serializer for {@link SavepointTriggerId}.
	 */
	public static class SavepointTriggerIdSerializer extends StdSerializer<SavepointTriggerId> {

		private static final long serialVersionUID = 1L;

		protected SavepointTriggerIdSerializer() {
			super(SavepointTriggerId.class);
		}

		@Override
		public void serialize(
				final SavepointTriggerId value,
				final JsonGenerator gen,
				final SerializerProvider provider) throws IOException {
			gen.writeString(value.toString());
		}
	}

	/**
	 * JSON deserializer for {@link SavepointTriggerId}.
	 */
	public static class SavepointTriggerIdDeserializer extends StdDeserializer<SavepointTriggerId> {

		private static final long serialVersionUID = 1L;

		protected SavepointTriggerIdDeserializer() {
			super(SavepointTriggerId.class);
		}

		@Override
		public SavepointTriggerId deserialize(
				final JsonParser p,
				final DeserializationContext ctxt) throws IOException {
			return SavepointTriggerId.fromHexString(p.getValueAsString());
		}
	}
}
