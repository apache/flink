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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A BLOB key uniquely identifies a BLOB.
 */
public final class BlobKey implements IOReadableWritable, Comparable<BlobKey> {

	/**
	 * Array of hex characters to facilitate fast toString() method.
	 */
	private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

	/**
	 * Size of the internal BLOB key in bytes.
	 */
	private static final int SIZE = 20;

	/**
	 * The byte buffer storing the actual key data.
	 */
	private final byte[] key;

	/**
	 * Constructs a new BLOB key.
	 */
	public BlobKey() {
		this.key = new byte[SIZE];
	}

	/**
	 * Constructs a new BLOB key from the given byte array.
	 * 
	 * @param key
	 *        the actual key data
	 */
	BlobKey(final byte[] key) {

		if (key.length != SIZE) {
			throw new IllegalArgumentException("BLOB key must have a size of " + SIZE + " bytes");
		}

		this.key = key;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutputView out) throws IOException {
		out.write(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInputView in) throws IOException {
		in.readFully(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof BlobKey)) {
			return false;
		}

		final BlobKey bk = (BlobKey) obj;

		return Arrays.equals(this.key, bk.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		// from http://stackoverflow.com/questions/9655181/convert-from-byte-array-to-hex-string-in-java
		final char[] hexChars = new char[SIZE * 2];
		for (int i = 0; i < SIZE; ++i) {
			int v = this.key[i] & 0xff;
			hexChars[i * 2] = HEX_ARRAY[v >>> 4];
			hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0f];
		}

		return new String(hexChars);
	}

	/**
	 * Auxiliary method to read a BLOB key from an input stream.
	 * 
	 * @param inputStream
	 *        the input stream to read the BLOB key from
	 * @return the read BLOB key
	 * @throws IOException
	 *         throw if an I/O error occurs while reading from the input stream
	 */
	static BlobKey readFromInputStream(final InputStream inputStream) throws IOException {

		final byte[] key = new byte[BlobKey.SIZE];

		int bytesRead = 0;
		while (bytesRead < BlobKey.SIZE) {
			final int read = inputStream.read(key, bytesRead, BlobKey.SIZE - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		return new BlobKey(key);
	}

	/**
	 * Auxiliary method to write this BLOB key to an output stream.
	 * 
	 * @param outputStream
	 *        the output stream to write the BLOB key to
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the BLOB key
	 */
	void writeToOutputStream(final OutputStream outputStream) throws IOException {

		outputStream.write(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final BlobKey o) {

		final byte[] aarr = this.key;
		final byte[] barr = o.key;
		final int len = Math.min(aarr.length, barr.length);

		for (int i = 0; i < len; ++i) {
			final int a = (aarr[i] & 0xff);
			final int b = (barr[i] & 0xff);
			if (a != b) {
				return a - b;
			}
		}

		return aarr.length - barr.length;
	}

	/**
	 * Adds the BLOB key to the given {@link MessageDigest}.
	 * 
	 * @param md
	 *        the message digest to add the BLOB key to
	 */
	public void addToMessageDigest(final MessageDigest md) {

		md.update(this.key);
	}
}
