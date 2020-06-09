/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@NotThreadSafe
class RefCountingFSDataInputStream extends FSDataInputStream {

	private enum State {NEW, OPENED, CLOSED}

	private final SupplierWithException<FSDataInputStream, IOException> streamSupplier;
	private FSDataInputStream stream;
	private final ChannelStateSerializer serializer;
	private int refCount = 0;
	private State state = State.NEW;

	private RefCountingFSDataInputStream(
			SupplierWithException<FSDataInputStream, IOException> streamSupplier,
			ChannelStateSerializer serializer) {
		this.streamSupplier = checkNotNull(streamSupplier);
		this.serializer = checkNotNull(serializer);
	}

	void incRef() {
		checkNotClosed();
		refCount++;
	}

	void decRef() throws IOException {
		checkNotClosed();
		refCount--;
		if (refCount == 0) {
			close();
		}
	}

	@Override
	public int read() throws IOException {
		ensureOpen();
		return stream.read();
	}

	@Override
	public void seek(long pos) throws IOException {
		ensureOpen();
		stream.seek(pos);
	}

	@Override
	public long getPos() throws IOException {
		ensureOpen();
		return stream.getPos();
	}

	public void close() throws IOException {
		state = State.CLOSED;
		if (stream != null) {
			stream.close();
			stream = null;
		}
	}

	private void ensureOpen() throws IOException {
		checkNotClosed();
		if (state == State.NEW) {
			stream = Preconditions.checkNotNull(streamSupplier.get());
			serializer.readHeader(stream);
			state = State.OPENED;
		}
	}

	private void checkNotClosed() {
		checkState(state != State.CLOSED, "stream is closed");
	}

	@NotThreadSafe
	static class RefCountingFSDataInputStreamFactory {
		private final Map<StreamStateHandle, RefCountingFSDataInputStream> streams = new HashMap<>(); // not clearing: expecting short life
		private final ChannelStateSerializer serializer;

		RefCountingFSDataInputStreamFactory(ChannelStateSerializer serializer) {
			this.serializer = checkNotNull(serializer);
		}

		<T> RefCountingFSDataInputStream getOrCreate(AbstractChannelStateHandle<T> handle) {
			StreamStateHandle streamStateHandle = handle.getDelegate();
			RefCountingFSDataInputStream stream = streams.get(streamStateHandle);
			if (stream == null) {
				stream = new RefCountingFSDataInputStream(streamStateHandle::openInputStream, serializer);
				streams.put(streamStateHandle, stream);
			}
			return stream;
		}

		ChannelStateSerializer getSerializer() {
			return serializer;
		}
	}

}
