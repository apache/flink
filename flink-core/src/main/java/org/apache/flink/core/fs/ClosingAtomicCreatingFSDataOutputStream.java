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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is a {@link org.apache.flink.util.WrappingProxy} for {@link AtomicCreatingFsDataOutputStream} that is used to
 * implement a safety net against unclosed streams.
 *
 * <p>See {@link SafetyNetCloseableRegistry} for more details on how this is utilized.
 */
@Internal
public class ClosingAtomicCreatingFSDataOutputStream
		extends AtomicCreatingFsDataOutputStream
		implements WrappingProxyCloseable<AtomicCreatingFsDataOutputStream> {

	private final SafetyNetCloseableRegistry registry;
	private final String debugString;
	private AtomicCreatingFsDataOutputStream outputStream;

	private AtomicBoolean closed;

	private ClosingAtomicCreatingFSDataOutputStream(
			AtomicCreatingFsDataOutputStream delegate, SafetyNetCloseableRegistry registry, String debugString) throws IOException {
		this.outputStream = delegate;
		this.registry = Preconditions.checkNotNull(registry);
		this.debugString = Preconditions.checkNotNull(debugString);
		this.closed = new AtomicBoolean(false);
	}

	public boolean isClosed() {
		return closed.get();
	}

	@Override
	public long getPos() throws IOException {
		return outputStream.getPos();
	}

	@Override
	public void write(int b) throws IOException {
		outputStream.write(b);
	}

	@Override
	public void flush() throws IOException {
		outputStream.flush();
	}

	@Override
	public void sync() throws IOException {
		outputStream.sync();
	}

	@Override
	public void close() throws IOException {
		if (closed.compareAndSet(false, true)) {
			registry.unregisterCloseable(this);
			outputStream.close();
		}
	}

	@Override
	public void closeAndPublish() throws IOException {
		if (closed.compareAndSet(false, true)) {
			registry.unregisterCloseable(this);
			outputStream.closeAndPublish();
		}
	}

	@Override
	public int hashCode() {
		return outputStream.hashCode();
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj instanceof ClosingAtomicCreatingFSDataOutputStream) {
			return outputStream.equals(((ClosingAtomicCreatingFSDataOutputStream) obj).outputStream);
		}

		return false;
	}

	@Override
	public String toString() {
		return "ClosingAtomicCreatingFSDataOutputStream(" + outputStream.toString() + ") : " + debugString;
	}

	public static ClosingAtomicCreatingFSDataOutputStream wrapSafe(
			AtomicCreatingFsDataOutputStream delegate, SafetyNetCloseableRegistry registry, String debugInfo) throws IOException {

		ClosingAtomicCreatingFSDataOutputStream outputStream = new ClosingAtomicCreatingFSDataOutputStream(delegate, registry, debugInfo);
		registry.registerCloseable(outputStream);
		return outputStream;
	}

	@Override
	public AtomicCreatingFsDataOutputStream getWrappedDelegate() {
		return outputStream;
	}
}
