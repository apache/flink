/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A {@link SimpleVersionedSerializer} implementation for Strings.
 */
@PublicEvolving
public final class SimpleVersionedStringSerializer implements SimpleVersionedSerializer<String> {

	private static final Charset CHARSET = StandardCharsets.UTF_8;

	public static final SimpleVersionedStringSerializer INSTANCE = new SimpleVersionedStringSerializer();

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public byte[] serialize(String value) {
		final byte[] serialized = value.getBytes(StandardCharsets.UTF_8);
		final byte[] targetBytes = new byte[Integer.BYTES + serialized.length];

		final ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
		bb.putInt(serialized.length);
		bb.put(serialized);
		return targetBytes;
	}

	@Override
	public String deserialize(int version, byte[] serialized) throws IOException {
		switch (version) {
			case 1:
				return deserializeV1(serialized);
			default:
				throw new IOException("Unrecognized version or corrupt state: " + version);
		}
	}

	private static String deserializeV1(byte[] serialized) {
		final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);
		final byte[] targetStringBytes = new byte[bb.getInt()];
		bb.get(targetStringBytes);
		return new String(targetStringBytes, CHARSET);
	}

	/**
	 * Private constructor to prevent instantiation.
	 * Access the serializer through the {@link #INSTANCE}.
	 */
	private SimpleVersionedStringSerializer() {}
}

