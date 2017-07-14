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

import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * A BLOB key uniquely identifies a BLOB.
 */
public final class BlobKey implements Serializable, Comparable<BlobKey> {

	private static final long serialVersionUID = 3847117712521785209L;

	/** Size of the internal BLOB key in bytes. */
	public static final int SIZE = 20;

	/** The byte buffer storing the actual key data. */
	private final byte[] key;

	/**
	 * Random component of the key.
	 */
	private final AbstractID random;

	/**
	 * Constructs a new BLOB key.
	 */
	public BlobKey() {
		this.key = new byte[SIZE];
		this.random = new AbstractID();
	}

	/**
	 * Constructs a new BLOB key from the given byte array.
	 *
	 * @param key
	 *        the actual key data
	 */
	BlobKey(byte[] key) {
		if (key == null || key.length != SIZE) {
			throw new IllegalArgumentException("BLOB key must have a size of " + SIZE + " bytes");
		}

		this.key = key;
		this.random = new AbstractID();
	}

	/**
	 * Constructs a new BLOB key from the given byte array.
	 * 
	 * @param key
	 *        the actual key data
	 * @param random
	 *        the random component of the key
	 */
	BlobKey(byte[] key, byte[] random) {
		if (key == null || key.length != SIZE) {
			throw new IllegalArgumentException("BLOB key must have a size of " + SIZE + " bytes");
		}

		this.key = key;
		this.random = new AbstractID(random);
	}

	/**
	 * Returns the hash component of this key.
	 *
	 * @return a 20 bit hash of the contents the key refers to
	 */
	byte[] getHash() {
		return key;
	}

	/**
	 * Adds the BLOB key to the given {@link MessageDigest}.
	 * 
	 * @param md
	 *        the message digest to add the BLOB key to
	 */
	public void addToMessageDigest(MessageDigest md) {
		md.update(this.key);
	}
	
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof BlobKey)) {
			return false;
		}

		final BlobKey bk = (BlobKey) obj;

		return Arrays.equals(this.key, bk.key) && this.random.equals(bk.random);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.key) + random.hashCode();
	}

	@Override
	public String toString() {
		return StringUtils.byteToHexString(this.key) + "-" + random.toString();
	}

	@Override
	public int compareTo(BlobKey o) {
		// compare the hashes first
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

		if (aarr.length == barr.length) {
			// same hash contents - continue and compare random components
			return this.random.compareTo(o.random);
		} else {
			return aarr.length - barr.length;
		}
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Auxiliary method to read a BLOB key from an input stream.
	 * 
	 * @param inputStream
	 *        the input stream to read the BLOB key from
	 * @return the read BLOB key
	 * @throws IOException
	 *         throw if an I/O error occurs while reading from the input stream
	 */
	static BlobKey readFromInputStream(InputStream inputStream) throws IOException {

		final byte[] key = new byte[BlobKey.SIZE];
		final byte[] random = new byte[AbstractID.SIZE];

		int bytesRead = 0;
		// read key
		while (bytesRead < BlobKey.SIZE) {
			final int read = inputStream.read(key, bytesRead, BlobKey.SIZE - bytesRead);
			if (read < 0) {
				throw new EOFException("Read an incomplete BLOB key");
			}
			bytesRead += read;
		}
		// read random component
		bytesRead = 0;
		while (bytesRead < AbstractID.SIZE) {
			final int read = inputStream.read(random, bytesRead, AbstractID.SIZE - bytesRead);
			if (read < 0) {
				throw new EOFException("Read an incomplete BLOB key");
			}
			bytesRead += read;
		}

		return new BlobKey(key, random);
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
		outputStream.write(this.random.getBytes());
	}
}
