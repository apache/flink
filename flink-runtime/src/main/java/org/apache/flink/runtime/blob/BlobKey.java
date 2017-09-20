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

import org.apache.flink.util.StringUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.apache.flink.runtime.blob.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A BLOB key uniquely identifies a BLOB.
 */
public final class BlobKey implements Serializable, Comparable<BlobKey> {

	private static final long serialVersionUID = 3847117712521785209L;

	/** Size of the internal BLOB key in bytes. */
	private static final int SIZE = 20;

	/** The byte buffer storing the actual key data. */
	private final byte[] key;

	private final BlobType type;

	/**
	 * Constructs a new BLOB key.
	 *
	 * @param type
	 * 		whether the referenced BLOB is permanent or transient
	 */
	public BlobKey(BlobType type) {
		this.type = checkNotNull(type);
		this.key = new byte[SIZE];
	}

	/**
	 * Constructs a new BLOB key from the given byte array.
	 *
	 * @param type
	 * 		whether the referenced BLOB is permanent or transient
	 * @param key
	 *        the actual key data
	 */
	BlobKey(BlobType type, byte[] key) {
		this.type = checkNotNull(type);

		if (key == null || key.length != SIZE) {
			throw new IllegalArgumentException("BLOB key must have a size of " + SIZE + " bytes");
		}

		this.key = key;
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
	 * Returns the referenced BLOB's type.
	 *
	 * @return whether the BLOB is permanent or transient
	 */
	public BlobType getType() {
		return type;
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

		return Arrays.equals(this.key, bk.key) && this.type == bk.type;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.key) + this.type.hashCode();
	}

	@Override
	public String toString() {
		final String typeString;
		switch (this.type) {
			case TRANSIENT_BLOB:
				typeString = "t-";
				break;
			case PERMANENT_BLOB:
				typeString = "p-";
				break;
			default:
				// this actually never happens!
				throw new IllegalStateException("Invalid BLOB type");
		}
		return typeString + StringUtils.byteToHexString(this.key);
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
			// same hash contents - compare the BLOB types
			return this.type.compareTo(o.type);
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

		int bytesRead = 0;
		// read key
		while (bytesRead < key.length) {
			final int read = inputStream.read(key, bytesRead, key.length - bytesRead);
			if (read < 0) {
				throw new EOFException("Read an incomplete BLOB key");
			}
			bytesRead += read;
		}
		// read BLOB type
		final BlobType blobType;
		{
			final int read = inputStream.read();
			if (read < 0) {
				throw new EOFException("Read an incomplete BLOB type");
			} else if (read == TRANSIENT_BLOB.ordinal()) {
				blobType = TRANSIENT_BLOB;
			} else if (read == PERMANENT_BLOB.ordinal()) {
				blobType = PERMANENT_BLOB;
			} else {
				throw new IOException("Invalid data received for the BLOB type: " + read);
			}
		}

		return new BlobKey(blobType, key);
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
		outputStream.write(this.type.ordinal());
	}
}
