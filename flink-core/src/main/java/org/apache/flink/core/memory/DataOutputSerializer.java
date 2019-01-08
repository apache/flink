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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * A simple and efficient serializer for the {@link java.io.DataOutput} interface.
 */
public class DataOutputSerializer implements DataOutputView {

	private static final Logger LOG = LoggerFactory.getLogger(DataOutputSerializer.class);

	private static final int PRUNE_BUFFER_THRESHOLD = 5 * 1024 * 1024;

	// ------------------------------------------------------------------------

	private final byte[] startBuffer;

	private byte[] buffer;

	private int position;

	private ByteBuffer wrapper;

	// ------------------------------------------------------------------------

	public DataOutputSerializer(int startSize) {
		if (startSize < 1) {
			throw new IllegalArgumentException();
		}

		this.startBuffer = new byte[startSize];
		this.buffer = this.startBuffer;
		this.wrapper = ByteBuffer.wrap(buffer);
	}

	public ByteBuffer wrapAsByteBuffer() {
		this.wrapper.position(0);
		this.wrapper.limit(this.position);
		return this.wrapper;
	}

	/**
	 * @deprecated Replaced by {@link #getSharedBuffer()} for a better, safer name.
	 */
	@Deprecated
	public byte[] getByteArray() {
		return getSharedBuffer();
	}

	/**
	 * Gets a reference to the internal byte buffer. This buffer may be larger than the
	 * actual serialized data. Only the bytes from zero to {@link #length()} are valid.
	 * The buffer will also be overwritten with the next write calls.
	 *
	 * <p>This method is useful when trying to avid byte copies, but should be used carefully.
	 *
	 * @return A reference to the internal shared and reused buffer.
	 */
	public byte[] getSharedBuffer() {
		return buffer;
	}

	/**
	 * Gets a copy of the buffer that has the right length for the data serialized so far.
	 * The returned buffer is an exclusive copy and can be safely used without being overwritten
	 * by future write calls to this serializer.
	 *
	 * <p>This method is equivalent to {@code Arrays.copyOf(getSharedBuffer(), length());}
	 *
	 * @return A non-shared copy of the serialization buffer.
	 */
	public byte[] getCopyOfBuffer() {
		return Arrays.copyOf(buffer, position);
	}

	public void clear() {
		this.position = 0;
	}

	public int length() {
		return this.position;
	}

	public void pruneBuffer() {
		if (this.buffer.length > PRUNE_BUFFER_THRESHOLD) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Releasing serialization buffer of " + this.buffer.length + " bytes.");
			}

			this.buffer = this.startBuffer;
			this.wrapper = ByteBuffer.wrap(this.buffer);
		}
	}

	@Override
	public String toString() {
		return String.format("[pos=%d cap=%d]", this.position, this.buffer.length);
	}

	// ----------------------------------------------------------------------------------------
	//                               Data Output
	// ----------------------------------------------------------------------------------------

	@Override
	public void write(int b) throws IOException {
		if (this.position >= this.buffer.length) {
			resize(1);
		}
		this.buffer[this.position++] = (byte) (b & 0xff);
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (len < 0 || off > b.length - len) {
			throw new ArrayIndexOutOfBoundsException();
		}
		if (this.position > this.buffer.length - len) {
			resize(len);
		}
		System.arraycopy(b, off, this.buffer, this.position, len);
		this.position += len;
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		write(v ? 1 : 0);
	}

	@Override
	public void writeByte(int v) throws IOException {
		write(v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		final int sLen = s.length();
		if (this.position >= this.buffer.length - sLen) {
			resize(sLen);
		}

		for (int i = 0; i < sLen; i++) {
			writeByte(s.charAt(i));
		}
		this.position += sLen;
	}

	@Override
	public void writeChar(int v) throws IOException {
		if (this.position >= this.buffer.length - 1) {
			resize(2);
		}
		this.buffer[this.position++] = (byte) (v >> 8);
		this.buffer[this.position++] = (byte) v;
	}

	@Override
	public void writeChars(String s) throws IOException {
		final int sLen = s.length();
		if (this.position >= this.buffer.length - 2 * sLen) {
			resize(2 * sLen);
		}
		for (int i = 0; i < sLen; i++) {
			writeChar(s.charAt(i));
		}
	}

	@Override
	public void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToLongBits(v));
	}

	@Override
	public void writeFloat(float v) throws IOException {
		writeInt(Float.floatToIntBits(v));
	}

	@SuppressWarnings("restriction")
	@Override
	public void writeInt(int v) throws IOException {
		if (this.position >= this.buffer.length - 3) {
			resize(4);
		}
		if (LITTLE_ENDIAN) {
			v = Integer.reverseBytes(v);
		}
		UNSAFE.putInt(this.buffer, BASE_OFFSET + this.position, v);
		this.position += 4;
	}

	@SuppressWarnings("restriction")
	@Override
	public void writeLong(long v) throws IOException {
		if (this.position >= this.buffer.length - 7) {
			resize(8);
		}
		if (LITTLE_ENDIAN) {
			v = Long.reverseBytes(v);
		}
		UNSAFE.putLong(this.buffer, BASE_OFFSET + this.position, v);
		this.position += 8;
	}

	@Override
	public void writeShort(int v) throws IOException {
		if (this.position >= this.buffer.length - 1) {
			resize(2);
		}
		this.buffer[this.position++] = (byte) ((v >>> 8) & 0xff);
		this.buffer[this.position++] = (byte) (v & 0xff);
	}

	@Override
	public void writeUTF(String str) throws IOException {
		int strlen = str.length();
		int utflen = 0;
		int c;

		/* use charAt instead of copying String to char array */
		for (int i = 0; i < strlen; i++) {
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				utflen++;
			} else if (c > 0x07FF) {
				utflen += 3;
			} else {
				utflen += 2;
			}
		}

		if (utflen > 65535) {
			throw new UTFDataFormatException("Encoded string is too long: " + utflen);
		}
		else if (this.position > this.buffer.length - utflen - 2) {
			resize(utflen + 2);
		}

		byte[] bytearr = this.buffer;
		int count = this.position;

		bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
		bytearr[count++] = (byte) (utflen & 0xFF);

		int i;
		for (i = 0; i < strlen; i++) {
			c = str.charAt(i);
			if (!((c >= 0x0001) && (c <= 0x007F))) {
				break;
			}
			bytearr[count++] = (byte) c;
		}

		for (; i < strlen; i++) {
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				bytearr[count++] = (byte) c;

			} else if (c > 0x07FF) {
				bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
				bytearr[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
				bytearr[count++] = (byte) (0x80 | (c & 0x3F));
			} else {
				bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
				bytearr[count++] = (byte) (0x80 | (c & 0x3F));
			}
		}

		this.position = count;
	}

	private void resize(int minCapacityAdd) throws IOException {
		int newLen = Math.max(this.buffer.length * 2, this.buffer.length + minCapacityAdd);
		byte[] nb;
		try {
			nb = new byte[newLen];
		}
		catch (NegativeArraySizeException e) {
			throw new IOException("Serialization failed because the record length would exceed 2GB (max addressable array size in Java).");
		}
		catch (OutOfMemoryError e) {
			// this was too large to allocate, try the smaller size (if possible)
			if (newLen > this.buffer.length + minCapacityAdd) {
				newLen = this.buffer.length + minCapacityAdd;
				try {
					nb = new byte[newLen];
				}
				catch (OutOfMemoryError ee) {
					// still not possible. give an informative exception message that reports the size
					throw new IOException("Failed to serialize element. Serialized size (> "
							+ newLen + " bytes) exceeds JVM heap space", ee);
				}
			} else {
				throw new IOException("Failed to serialize element. Serialized size (> "
						+ newLen + " bytes) exceeds JVM heap space", e);
			}
		}

		System.arraycopy(this.buffer, 0, nb, 0, this.position);
		this.buffer = nb;
		this.wrapper = ByteBuffer.wrap(this.buffer);
	}

	@Override
	public void skipBytesToWrite(int numBytes) throws IOException {
		if (buffer.length - this.position < numBytes){
			throw new EOFException("Could not skip " + numBytes + " bytes.");
		}

		this.position += numBytes;
	}

	@Override
	public void write(DataInputView source, int numBytes) throws IOException {
		if (buffer.length - this.position < numBytes){
			throw new EOFException("Could not write " + numBytes + " bytes. Buffer overflow.");
		}

		source.readFully(this.buffer, this.position, numBytes);
		this.position += numBytes;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@SuppressWarnings("restriction")
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

	@SuppressWarnings("restriction")
	private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

	private static final boolean LITTLE_ENDIAN = (MemoryUtils.NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN);
}
