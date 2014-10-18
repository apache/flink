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

package org.apache.flink.runtime.blob;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

/**
 * The BLOB input stream is a special implementation of an {@link InputStream} to read the results of a GET operation
 * from the BLOB server.
 */
final class BlobInputStream extends InputStream {

	/**
	 * The wrapped input stream from the underlying TCP connection.
	 */
	private final InputStream wrappedInputStream;

	/**
	 * The BLOB key if the GET operation has been performed on a content-addressable BLOB, otherwise <code>null<code>.
	 */
	private final BlobKey blobKey;

	/**
	 * The number of bytes to read from the underlying input stream before indicating an end-of-stream.
	 */
	private final int bytesToReceive;

	/**
	 * The message digest to verify the integrity of the retrieved content-addressable BLOB. If the BLOB is
	 * non-content-addressable, this is <code>null</code>.
	 */
	private final MessageDigest md;

	/**
	 * The number of bytes already read from the underlying input stream.
	 */
	private int bytesReceived;

	/**
	 * Constructs a new BLOB input stream.
	 * 
	 * @param wrappedInputStream
	 *        the underlying input stream to read from
	 * @param blobKey
	 *        the expected BLOB key for content-addressable BLOBs, <code>null</code> for non-content-addressable BLOBs.
	 * @param buf
	 *        auxiliary buffer to read the meta data from the BLOB server
	 * @throws IOException
	 *         throws if an I/O error occurs while reading the BLOB data from the BLOB server
	 */
	BlobInputStream(final InputStream wrappedInputStream, final BlobKey blobKey, final byte[] buf) throws IOException {

		this.wrappedInputStream = wrappedInputStream;
		this.blobKey = blobKey;
		this.bytesToReceive = BlobServer.readLength(buf, wrappedInputStream);
		if (this.bytesToReceive < 0) {
			throw new FileNotFoundException();
		}

		this.md = (blobKey != null) ? BlobUtils.createMessageDigest() : null;
	}

	/**
	 * Convenience method to throw an {@link EOFException}.
	 * 
	 * @throws EOFException
	 *         thrown to indicate the underlying input stream did not provide as much data as expected
	 */
	private void throwEOFException() throws EOFException {
		throw new EOFException(String.format("Expected to read %d more bytes from stream", this.bytesToReceive
			- this.bytesReceived));
	}

	@Override
	public int read() throws IOException {

		if (this.bytesReceived == this.bytesToReceive) {
			return -1;
		}

		final int read = this.wrappedInputStream.read();
		if (read < 0) {
			throwEOFException();
		}

		++this.bytesReceived;

		if (this.md != null) {
			this.md.update((byte) read);
			if (this.bytesReceived == this.bytesToReceive) {
				final BlobKey computedKey = new BlobKey(this.md.digest());
				if (!computedKey.equals(this.blobKey)) {
					throw new IOException("Detected data corruption during transfer");
				}
			}
		}

		return read;
	}

	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {

		final int bytesMissing = this.bytesToReceive - this.bytesReceived;

		if (bytesMissing == 0) {
			return -1;
		}

		final int maxRecv = Math.min(len, bytesMissing);
		final int read = this.wrappedInputStream.read(b, off, maxRecv);
		if (read < 0) {
			throwEOFException();
		}

		this.bytesReceived += read;

		if (this.md != null) {
			this.md.update(b, off, read);
			if (this.bytesReceived == this.bytesToReceive) {
				final BlobKey computedKey = new BlobKey(this.md.digest());
				if (!computedKey.equals(this.blobKey)) {
					throw new IOException("Detected data corruption during transfer");
				}
			}
		}

		return read;
	}

	@Override
	public long skip(long n) throws IOException {

		return 0L;
	}

	@Override
	public int available() throws IOException {

		return 0;
	}

	@Override
	public void close() throws IOException {
		// This method does not do anything as the wrapped input stream may be used for multiple get operations.
	}

	public void mark(final int readlimit) {

		// Do not do anything here
	}

	@Override
	public void reset() throws IOException {

		throw new IOException("mark/reset not supported");
	}

	@Override
	public boolean markSupported() {

		return false;
	}
}
