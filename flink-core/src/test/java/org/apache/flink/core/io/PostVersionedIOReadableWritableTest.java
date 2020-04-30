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

package org.apache.flink.core.io;

import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Suite of tests for {@link PostVersionedIOReadableWritable}.
 */
public class PostVersionedIOReadableWritableTest {

	@Test
	public void testReadVersioned() throws IOException {

		String payload = "test-data";
		TestPostVersionedReadableWritable versionedReadableWritable = new TestPostVersionedReadableWritable(payload);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			versionedReadableWritable.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		TestPostVersionedReadableWritable restoredVersionedReadableWritable = new TestPostVersionedReadableWritable();
		try(ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			restoredVersionedReadableWritable.read(in);
		}

		Assert.assertEquals(payload, restoredVersionedReadableWritable.getData());
	}

	@Test
	public void testReadNonVersioned() throws IOException {
		int preVersionedPayload = 563;

		TestNonVersionedReadableWritable nonVersionedReadableWritable = new TestNonVersionedReadableWritable(preVersionedPayload);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			nonVersionedReadableWritable.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		TestPostVersionedReadableWritable restoredVersionedReadableWritable = new TestPostVersionedReadableWritable();
		try(ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			restoredVersionedReadableWritable.read(in);
		}

		Assert.assertEquals(String.valueOf(preVersionedPayload), restoredVersionedReadableWritable.getData());
	}

	static class TestPostVersionedReadableWritable extends PostVersionedIOReadableWritable {

		private static final int VERSION = 1;
		private String data;

		TestPostVersionedReadableWritable() {}

		TestPostVersionedReadableWritable(String data) {
			this.data = data;
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);
			out.writeUTF(data);
		}

		@Override
		protected void read(DataInputView in, boolean wasVersioned) throws IOException {
			if (wasVersioned) {
				this.data = in.readUTF();
			} else {
				// in the previous non-versioned format, we wrote integers instead
				this.data = String.valueOf(in.readInt());
			}
		}

		public String getData() {
			return data;
		}
	}

	static class TestNonVersionedReadableWritable implements IOReadableWritable {

		private int data;

		TestNonVersionedReadableWritable(int data) {
			this.data = data;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeInt(data);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			this.data = in.readInt();
		}
	}

}
