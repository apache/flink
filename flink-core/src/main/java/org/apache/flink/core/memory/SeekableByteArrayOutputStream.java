/**
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * A seekable variant of {@link ByteArrayOutputStream}.
 */
public class SeekableByteArrayOutputStream extends ByteArrayOutputStream {

	/**
	 * Creates an underlying buffer of the specified size.
	 */
	public SeekableByteArrayOutputStream(int size) {
		super(size);
	}

	/**
	 * Returns the current writing position.
	 */
	public int tell() {
		return count;
	}

	/**
	 * Sets the current writing position.
	 */
	public void seek(int to) throws IOException {

		if (to > buf.length - 1) {
			// leave some room
			resize((int)(to * 1.5));
		}

		// absolute position
		count = to;
	}

	// we need this for seek() since ensure() and grow() are private in ByteArrayOutputStream
	private final void resize(int minCapacityAdd) throws IOException {
		final int newLen = Math.max(buf.length * 2, buf.length + minCapacityAdd);
		final byte[] nb = new byte[newLen];
		try {
			// copy the whole buffer since we might have data after the current position
			// due to seeking
			System.arraycopy(buf, 0, nb, 0, buf.length);
			buf = nb;
		}
		catch (NegativeArraySizeException nasex) {
			throw new IOException("Resized buffer would exceed 2GB (max addressable array size in Java).");
		}
	}

	@Override
	public int size() {
		return count;
	}

	@Override
	public synchronized void reset() {
		count = 0;
	}

	@Override
	public synchronized byte toByteArray()[] {
		return Arrays.copyOf(buf, count);
	}
}
