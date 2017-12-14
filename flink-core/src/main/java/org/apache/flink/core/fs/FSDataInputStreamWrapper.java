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
import org.apache.flink.util.WrappingProxy;

import java.io.IOException;

/**
 * Simple forwarding wrapper around {@link FSDataInputStream}.
 */
@Internal
public class FSDataInputStreamWrapper extends FSDataInputStream implements WrappingProxy<FSDataInputStream> {

	protected final FSDataInputStream inputStream;

	public FSDataInputStreamWrapper(FSDataInputStream inputStream) {
		this.inputStream = Preconditions.checkNotNull(inputStream);
	}

	@Override
	public void seek(long desired) throws IOException {
		inputStream.seek(desired);
	}

	@Override
	public long getPos() throws IOException {
		return inputStream.getPos();
	}

	@Override
	public int read() throws IOException {
		return inputStream.read();
	}

	@Override
	public int read(byte[] b) throws IOException {
		return inputStream.read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return inputStream.read(b, off, len);
	}

	@Override
	public long skip(long n) throws IOException {
		return inputStream.skip(n);
	}

	@Override
	public int available() throws IOException {
		return inputStream.available();
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
	}

	@Override
	public void mark(int readlimit) {
		inputStream.mark(readlimit);
	}

	@Override
	public void reset() throws IOException {
		inputStream.reset();
	}

	@Override
	public boolean markSupported() {
		return inputStream.markSupported();
	}

	@Override
	public FSDataInputStream getWrappedDelegate() {
		return inputStream;
	}
}
