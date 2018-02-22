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

package org.apache.flink.runtime.rest.messages;

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
@JsonSerialize(using = TriggerId.TriggerIdSerializer.class)
@JsonDeserialize(using = TriggerId.TriggerIdDeserializer.class)
public final class TriggerId extends AbstractID {

	private static final long serialVersionUID = 1L;

	public TriggerId() {
	}

	private TriggerId(final byte[] bytes) {
		super(bytes);
	}

	public static TriggerId fromHexString(String hexString) {
		return new TriggerId(DatatypeConverter.parseHexBinary(hexString));
	}

	/**
	 * JSON serializer for {@link TriggerId}.
	 */
	public static class TriggerIdSerializer extends StdSerializer<TriggerId> {

		private static final long serialVersionUID = 1L;

		protected TriggerIdSerializer() {
			super(TriggerId.class);
		}

		@Override
		public void serialize(
				final TriggerId value,
				final JsonGenerator gen,
				final SerializerProvider provider) throws IOException {
			gen.writeString(value.toString());
		}
	}

	/**
	 * JSON deserializer for {@link TriggerId}.
	 */
	public static class TriggerIdDeserializer extends StdDeserializer<TriggerId> {

		private static final long serialVersionUID = 1L;

		protected TriggerIdDeserializer() {
			super(TriggerId.class);
		}

		@Override
		public TriggerId deserialize(
				final JsonParser p,
				final DeserializationContext ctxt) throws IOException {
			return TriggerId.fromHexString(p.getValueAsString());
		}
	}
}
