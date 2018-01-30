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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class that turns a {@link DataInputView} into an {@link InputStream}.
 */
@Internal
public class InputStreamViewWrapper extends InputStream {

	/** Maximum number of bytes that is allowed to be skipped at once on the wrapped input view. */
	private static final int MAX_SKIP_SIZE = 2048;

	private final DataInputView inputView;

	public InputStreamViewWrapper(DataInputView inputView) {
		this.inputView = checkNotNull(inputView);
	}

	@Override
	public int read() throws IOException {
		return inputView.readByte();
	}

	/**
	 * The underlying {@code InputStream} of the wrapped input view may not have an implementation of
	 * this method that relies on {@link #read()}.
	 *
	 * <p>Specifically forwarding this call to the wrapped input view ensures that the actual read implementation
	 * is invoked.
	 */
	@Override
	public int read(byte[] b) throws IOException {
		return inputView.read(b);
	}

	/**
	 * The underlying {@code InputStream} of the wrapped input view may not have an implementation of
	 * this method that relies on {@link #read()}.
	 *
	 * <p>Specifically forwarding this call to the wrapped input view ensures that the actual read implementation
	 * is invoked.
	 */
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return inputView.read(b, off, len);
	}

	/**
	 * This is a re-implementation of the base {@link InputStream#skip(long)} method, that
	 * directly uses the {@link DataInputView#skipBytes(int)} call on the wrapped input view,
	 * instead of relying on {@link #read()}.
	 *
	 * <p>Specifically forwarding this call to the wrapped input view ensures that the actual skip implementation
	 * is invoked.
	 */
	@Override
	public long skip(long n) throws IOException {
		long remaining = n;
		int nr;

		if (n <= 0) {
			return 0;
		}

		int size = (int) Math.min(MAX_SKIP_SIZE, remaining);
		while (remaining > 0) {
			nr = inputView.skipBytes((int) Math.min(size, remaining));
			if (nr <= 0) {
				break;
			}
			remaining -= nr;
		}

		return n - remaining;
	}
}
