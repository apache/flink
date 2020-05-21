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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * Wrapper around a {@link StreamStateHandle} to make the referenced state object retrievable trough a simple get call.
 * This implementation expects that the object was serialized through default serialization of Java's
 * {@link java.io.ObjectOutputStream}.
 *
 * @param <T> type of the retrievable object which is stored under the wrapped stream handle
 */
public class RetrievableStreamStateHandle<T extends Serializable> implements
		StreamStateHandle, RetrievableStateHandle<T>, Closeable {

	private static final long serialVersionUID = 314567453677355L;

	/** wrapped inner stream state handle from which we deserialize on retrieval */
	private final StreamStateHandle wrappedStreamStateHandle;

	public RetrievableStreamStateHandle(StreamStateHandle streamStateHandle) {
		this.wrappedStreamStateHandle = Preconditions.checkNotNull(streamStateHandle);
	}

	public RetrievableStreamStateHandle(Path filePath, long stateSize) {
		Preconditions.checkNotNull(filePath);
		this.wrappedStreamStateHandle = new FileStateHandle(filePath, stateSize);
	}

	@Override
	public T retrieveState() throws IOException, ClassNotFoundException {
		try (FSDataInputStream in = openInputStream()) {
			return InstantiationUtil.deserializeObject(in, Thread.currentThread().getContextClassLoader());
		}
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return wrappedStreamStateHandle.openInputStream();
	}

	@Override
	public Optional<byte[]> asBytesIfInMemory() {
		return wrappedStreamStateHandle.asBytesIfInMemory();
	}

	@Override
	public void discardState() throws Exception {
		wrappedStreamStateHandle.discardState();
	}

	@Override
	public long getStateSize() {
		return wrappedStreamStateHandle.getStateSize();
	}

	@Override
	public void close() throws IOException {
//		wrappedStreamStateHandle.close();
	}
}
