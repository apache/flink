/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.io.InputStream;

final class SinglePacketInputStream extends InputStream {

	private final byte[] buf;

	private final int len;

	private int read;

	SinglePacketInputStream(final byte[] buf, final int len) {

		this.buf = buf;
		this.read = 0;
		this.len = len;
	}

	@Override
	public int available() {
		return (this.len - this.read);
	}

	@Override
	public void close() {
		// Nothing to do here
	}

	@Override
	public void mark(final int readlimit) {
		// Nothing to do here
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	@Override
	public int read() throws IOException {

		if (this.read == this.len) {
			return -1;
		}

		return this.buf[this.read++];
	}

	@Override
	public int read(final byte[] b) {

		return read(b, 0, b.length);
	}

	@Override
	public int read(final byte[] b, final int off, final int len) {

		if (this.read == this.len) {
			return -1;
		}

		final int r = Math.min(len, this.len - this.read);
		System.arraycopy(this.buf, this.read, b, off, r);
		this.read += r;

		return r;
	}

	@Override
	public void reset() {
		this.read = 0;
	}

	@Override
	public long skip(long n) {

		final int dataLeftInBuffer = this.len - this.read;

		if (n > dataLeftInBuffer) {
			this.read = this.len;
			return dataLeftInBuffer;
		}

		this.read += (int) n;

		return n;
	}
}
