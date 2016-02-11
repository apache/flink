/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.flink.runtime.state.AbstractStateBackend.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

/**
 * This implementation of the {@link CheckpointStateOutputStream} will write the
 * data to a {@link ByteArrayOutputStream} and when the user calls
 * {@link #closeAndGetHandle()}. It converts the byte array to a serializable
 * checkpoint. This is necessary because we cannot directly stream data into the
 * database.
 * 
 * Similarly when the input stream is retrieved from the checkpoint the byte
 * array is deserialized and wrapped into a {@link ByteArrayInputStream}.
 *
 */
public class DbStateOutputStream extends CheckpointStateOutputStream {

	private transient final DbStateBackend backend;

	private final long checkpointId;
	private final long checkpointTs;

	private transient final ByteArrayOutputStream outStream;

	public DbStateOutputStream(DbStateBackend backend, long checkpointId, long checkpointTs) {
		this.backend = backend;
		this.checkpointId = checkpointId;
		this.checkpointTs = checkpointTs;
		this.outStream = new ByteArrayOutputStream();
	}

	@Override
	public StreamStateHandle closeAndGetHandle() throws IOException {
		try (ByteArrayOutputStream out = outStream) {
			byte[] bytes = out.toByteArray();
			return new DbStreamStateHandle(
					backend.checkpointStateSerializable(bytes, checkpointId, checkpointTs), bytes.length);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void write(int b) throws IOException {
		outStream.write(b);
	}

	private static class DbStreamStateHandle implements StreamStateHandle {
		private static final long serialVersionUID = 1L;
		private final StateHandle<byte[]> handle;
		private final int size;

		private DbStreamStateHandle(StateHandle<byte[]> handle, int stateSize) {
			this.handle = handle;
			this.size = stateSize;
		}

		@Override
		public InputStream getState(ClassLoader userCodeClassLoader) throws Exception {
			return new ByteArrayInputStream(handle.getState(userCodeClassLoader));
		}

		@Override
		public void discardState() throws Exception {
			handle.discardState();
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T extends Serializable> StateHandle<T> toSerializableHandle() {
			return (StateHandle<T>) handle;
		}

		@Override
		public long getStateSize() throws Exception {
			return size;
		}

	}

}
