/**
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

package org.apache.flink.core.memory;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class OutputViewDataOutputWrapper implements DataOutputView {
	private static final int INITIAL_LOCK_BUFFER_SIZE = 4096;

	private DataOutput out;

	private DataOutput originalOut = null;
	private SeekableByteArrayOutputStream seekableBuffer = null;
	private DataOutputStream seekableOut = null;

	// We need to keep track of stacked locking, i.e. when child serializers also lock and unlock.
	private int lockCount = 0;

	public OutputViewDataOutputWrapper(DataOutput out) throws IOException {
		this.out = out;

		this.originalOut = out;
		this.seekableBuffer = new SeekableByteArrayOutputStream(INITIAL_LOCK_BUFFER_SIZE);
		this.seekableOut = new DataOutputStream(this.seekableBuffer);
	}

	@Override
	public void lock() {
		lockCount++;
		out = seekableOut;

		if (lockCount == 1) {
			// first lock
			seekableBuffer.reset();
		}
	}

	@Override
	public void unlock() throws IOException {
		lockCount--;

		if (lockCount < 0) {
			throw new RuntimeException("More unlock calls than previous lock calls.");
		}

		if (lockCount == 0) {
			out = originalOut;
			byte[] bytes = seekableBuffer.toByteArray();
			originalOut.write(bytes);
			seekableBuffer.reset();
		}
	}

	@Override
	public long tell() throws IOException {
		if (lockCount <= 0) {
			throw new RuntimeException("Buffer must be locked.");
		}
		return seekableBuffer.tell();
	}

	@Override
	public void seek(long position) throws IOException {
		if (lockCount <= 0) {
			throw new RuntimeException("Buffer must be locked.");
		}
		seekableBuffer.seek((int)position);
	}

	@Override
	public void skipBytesToWrite(int numBytes) throws IOException {
		byte[] buffer = new byte[numBytes];
		Arrays.fill(buffer, (byte) 0);

		out.write(buffer);
	}

	@Override
	public void write(DataInputView source, int numBytes) throws IOException {
		byte[] buffer = new byte[numBytes];
		source.readFully(buffer);
		out.write(buffer);
	}

	@Override
	public void write(int b) throws IOException {
		out.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		out.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		out.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) throws IOException {
		out.writeByte(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		out.writeShort(v);
	}

	@Override
	public void writeChar(int v) throws IOException {
		out.writeChar(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		out.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		out.writeLong(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		out.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		out.writeDouble(v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		out.writeBytes(s);
	}

	@Override
	public void writeChars(String s) throws IOException {
		out.writeChars(s);
	}

	@Override
	public void writeUTF(String s) throws IOException {
		out.writeUTF(s);
	}
}
