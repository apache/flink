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

/**
 * This class is a {@link org.apache.flink.util.WrappingProxy} for {@link FSDataOutputStream} that is used to
 * implement a safety net against unclosed streams.
 *
 * <p>See {@link SafetyNetCloseableRegistry} for more details on how this is utilized.
 */
@Internal
public class ClosingFSDataOutputStream
		extends FSDataOutputStreamWrapper
		implements WrappingProxyCloseable<FSDataOutputStream> {

	private final SafetyNetCloseableRegistry registry;
	private final String debugString;

	private volatile boolean closed;

	public ClosingFSDataOutputStream(
			FSDataOutputStream delegate, SafetyNetCloseableRegistry registry) throws IOException {
		this(delegate, registry, "");
	}

	private ClosingFSDataOutputStream(
			FSDataOutputStream delegate, SafetyNetCloseableRegistry registry, String debugString) throws IOException {
		super(delegate);
		this.registry = Preconditions.checkNotNull(registry);
		this.debugString = Preconditions.checkNotNull(debugString);
		this.closed = false;
	}

	public boolean isClosed() {
		return closed;
	}

	@Override
	public void close() throws IOException {
		if (!closed) {
			closed = true;
			registry.unregisterCloseable(this);
			outputStream.close();
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

		if (obj instanceof ClosingFSDataOutputStream) {
			return outputStream.equals(((ClosingFSDataOutputStream) obj).outputStream);
		}

		return false;
	}

	@Override
	public String toString() {
		return "ClosingFSDataOutputStream(" + outputStream.toString() + ") : " + debugString;
	}

	public static ClosingFSDataOutputStream wrapSafe(
			FSDataOutputStream delegate, SafetyNetCloseableRegistry registry) throws IOException {
		return wrapSafe(delegate, registry, "");
	}

	public static ClosingFSDataOutputStream wrapSafe(
			FSDataOutputStream delegate, SafetyNetCloseableRegistry registry, String debugInfo) throws IOException {

		ClosingFSDataOutputStream inputStream = new ClosingFSDataOutputStream(delegate, registry, debugInfo);
		registry.registerCloseable(inputStream);
		return inputStream;
	}
}
