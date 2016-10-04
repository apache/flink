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

package org.apache.flink.runtime.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Decorator for input streams that ignores calls to {@link InputStream#close()}.
 */
public class NonClosingStreamDecorator extends InputStream {

	private final InputStream delegate;

	public NonClosingStreamDecorator(InputStream delegate) {
		this.delegate = delegate;
	}

	@Override
	public int read() throws IOException {
		return delegate.read();
	}

	@Override
	public int read(byte[] b) throws IOException {
		return delegate.read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return delegate.read(b, off, len);
	}

	@Override
	public long skip(long n) throws IOException {
		return delegate.skip(n);
	}

	@Override
	public int available() throws IOException {
		return super.available();
	}

	@Override
	public void close() throws IOException {
		// ignore
	}

	@Override
	public void mark(int readlimit) {
		super.mark(readlimit);
	}

	@Override
	public void reset() throws IOException {
		super.reset();
	}

	@Override
	public boolean markSupported() {
		return super.markSupported();
	}
}