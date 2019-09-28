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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link CheckpointStreamFactory} that produces streams that write to in-memory byte arrays.
 */
public class MemCheckpointStreamFactory implements CheckpointStreamFactory {

	/** The maximal size that the snapshotted memory state may have */
	private final int maxStateSize;

	/**
	 * Creates a new in-memory stream factory that accepts states whose serialized forms are
	 * up to the given number of bytes.
	 *
	 * @param maxStateSize The maximal size of the serialized state
	 */
	public MemCheckpointStreamFactory(int maxStateSize) {
		this.maxStateSize = maxStateSize;
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(
			CheckpointedStateScope scope) throws IOException
	{
		return new MemoryCheckpointOutputStream(maxStateSize);
	}

	@Override
	public String toString() {
		return "In-Memory Stream Factory";
	}

	static void checkSize(int size, int maxSize) throws IOException {
		if (size > maxSize) {
			throw new IOException(
					"Size of the state is larger than the maximum permitted memory-backed state. Size="
							+ size + " , maxSize=" + maxSize
							+ " . Consider using a different state backend, like the File System State backend.");
		}
	}



	/**
	 * A {@code CheckpointStateOutputStream} that writes into a byte array.
	 */
	public static class MemoryCheckpointOutputStream extends CheckpointStateOutputStream {

		private final ByteArrayOutputStreamWithPos os = new ByteArrayOutputStreamWithPos();

		private final int maxSize;

		private AtomicBoolean closed;

		boolean isEmpty = true;

		public MemoryCheckpointOutputStream(int maxSize) {
			this.maxSize = maxSize;
			this.closed = new AtomicBoolean(false);
		}

		@Override
		public void write(int b) throws IOException {
			os.write(b);
			isEmpty = false;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			os.write(b, off, len);
			isEmpty = false;
		}

		@Override
		public void flush() throws IOException {
			os.flush();
		}

		@Override
		public void sync() throws IOException { }

		// --------------------------------------------------------------------

		@Override
		public void close() {
			if (closed.compareAndSet(false, true)) {
				closeInternal();
			}
		}

		@Nullable
		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			if (isEmpty) {
				return null;
			}
			return new ByteStreamStateHandle(String.valueOf(UUID.randomUUID()), closeAndGetBytes());
		}

		@Override
		public long getPos() throws IOException {
			return os.getPosition();
		}

		public boolean isClosed() {
			return closed.get();
		}

		/**
		 * Closes the stream and returns the byte array containing the stream's data.
		 * @return The byte array containing the stream's data.
		 * @throws IOException Thrown if the size of the data exceeds the maximal
		 */
		public byte[] closeAndGetBytes() throws IOException {
			if (closed.compareAndSet(false, true)) {
				checkSize(os.size(), maxSize);
				byte[] bytes = os.toByteArray();
				closeInternal();
				return bytes;
			} else {
				throw new IOException("stream has already been closed");
			}
		}

		private void closeInternal() {
			os.reset();
		}
	}
}
