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

package org.apache.flink.runtime.net.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.*;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.http.router.Router;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Objects;

/**
 */
class TestUtils {
	public static final ObjectMapper mapper = new ObjectMapper();

	public static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

	public static final Router router = new Router().ANY("/", (ChannelInboundHandler) null);

	public static <T> T deserialize(ByteBuf byteBuf, Charset charset, Class<T> clazz) throws IOException {
		try (Reader reader = new InputStreamReader(new ByteBufInputStream(byteBuf), charset)) {
			return mapper.readValue(reader, clazz);
		}
	}

	public static ByteBuf serialize(Object value, Charset charset) throws IOException {
		ByteBuf byteBuf = allocator.buffer();
		try (Writer stream = new OutputStreamWriter(new ByteBufOutputStream(byteBuf))) {
			mapper.writeValue(stream, value);
		}
		return byteBuf;
	}

	public static class TestEntity {
		private int x;
		@JsonCreator
		public TestEntity(@JsonProperty("x") int x) {
			this.x = x;
		}
		@JsonProperty("x")
		public int getX() { return x; }

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestEntity that = (TestEntity) o;
			return x == that.x;
		}

		@Override
		public int hashCode() {
			return Objects.hash(x);
		}
	}

	public static class UnreadableTestEntity extends TestEntity {
		public UnreadableTestEntity(@JsonProperty("x") int x) {
			super(x);
			throw new UnsupportedOperationException();
		}
	}

	public static class UnwritableTestEntity extends TestEntity {
		public UnwritableTestEntity() {
			super(0);
		}

		@Override
		public int getX() {
			throw new UnsupportedOperationException();
		}
	}
}
