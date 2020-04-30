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

package org.apache.flink.core.fs.local;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Simple serializer for the {@link LocalRecoverable}.
 */
@Internal
class LocalRecoverableSerializer implements SimpleVersionedSerializer<LocalRecoverable> {

	static final LocalRecoverableSerializer INSTANCE = new LocalRecoverableSerializer();

	private static final Charset CHARSET = StandardCharsets.UTF_8;

	private static final int MAGIC_NUMBER = 0x1e744b57;

	/**
	 * Do not instantiate, use reusable {@link #INSTANCE} instead.
	 */
	private LocalRecoverableSerializer() {}

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public byte[] serialize(LocalRecoverable obj) throws IOException {
		final byte[] targetFileBytes = obj.targetFile().getAbsolutePath().getBytes(CHARSET);
		final byte[] tempFileBytes = obj.tempFile().getAbsolutePath().getBytes(CHARSET);
		final byte[] targetBytes = new byte[20 + targetFileBytes.length + tempFileBytes.length];

		ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
		bb.putInt(MAGIC_NUMBER);
		bb.putLong(obj.offset());
		bb.putInt(targetFileBytes.length);
		bb.putInt(tempFileBytes.length);
		bb.put(targetFileBytes);
		bb.put(tempFileBytes);

		return targetBytes;
	}

	@Override
	public LocalRecoverable deserialize(int version, byte[] serialized) throws IOException {
		switch (version) {
			case 1:
				return deserializeV1(serialized);
			default:
				throw new IOException("Unrecognized version or corrupt state: " + version);
		}
	}

	private static LocalRecoverable deserializeV1(byte[] serialized) throws IOException {
		final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

		if (bb.getInt() != MAGIC_NUMBER) {
			throw new IOException("Corrupt data: Unexpected magic number.");
		}

		final long offset = bb.getLong();
		final byte[] targetFileBytes = new byte[bb.getInt()];
		final byte[] tempFileBytes = new byte[bb.getInt()];
		bb.get(targetFileBytes);
		bb.get(tempFileBytes);

		final String targetPath = new String(targetFileBytes, CHARSET);
		final String tempPath = new String(tempFileBytes, CHARSET);

		return new LocalRecoverable(new File(targetPath), new File(tempPath), offset);

	}
}
