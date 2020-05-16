/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Optional;

final class NonSpanningWrapper implements DataInputView {

	MemorySegment segment;

	private int limit;

	int position;

	private byte[] utfByteBuffer; // reusable byte buffer for utf-8 decoding
	private char[] utfCharBuffer; // reusable char buffer for utf-8 decoding

	int remaining() {
		return this.limit - this.position;
	}

	void clear() {
		this.segment = null;
		this.limit = 0;
		this.position = 0;
	}

	void initializeFromMemorySegment(MemorySegment seg, int position, int leftOverLimit) {
		this.segment = seg;
		this.position = position;
		this.limit = leftOverLimit;
	}

	Optional<MemorySegment> getUnconsumedSegment() {
		if (remaining() == 0) {
			return Optional.empty();
		}
		MemorySegment target = MemorySegmentFactory.allocateUnpooledSegment(remaining());
		segment.copyTo(position, target, 0, remaining());
		return Optional.of(target);
	}

	// -------------------------------------------------------------------------------------------------------------
	//                                       DataInput specific methods
	// -------------------------------------------------------------------------------------------------------------

	@Override
	public final void readFully(byte[] b) {
		readFully(b, 0, b.length);
	}

	@Override
	public final void readFully(byte[] b, int off, int len) {
		if (off < 0 || len < 0 || off + len > b.length) {
			throw new IndexOutOfBoundsException();
		}

		this.segment.get(this.position, b, off, len);
		this.position += len;
	}

	@Override
	public final boolean readBoolean() {
		return readByte() == 1;
	}

	@Override
	public final byte readByte() {
		return this.segment.get(this.position++);
	}

	@Override
	public final int readUnsignedByte() {
		return readByte() & 0xff;
	}

	@Override
	public final short readShort() {
		final short v = this.segment.getShortBigEndian(this.position);
		this.position += 2;
		return v;
	}

	@Override
	public final int readUnsignedShort() {
		final int v = this.segment.getShortBigEndian(this.position) & 0xffff;
		this.position += 2;
		return v;
	}

	@Override
	public final char readChar()  {
		final char v = this.segment.getCharBigEndian(this.position);
		this.position += 2;
		return v;
	}

	@Override
	public final int readInt() {
		final int v = this.segment.getIntBigEndian(this.position);
		this.position += 4;
		return v;
	}

	@Override
	public final long readLong() {
		final long v = this.segment.getLongBigEndian(this.position);
		this.position += 8;
		return v;
	}

	@Override
	public final float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public final double readDouble() {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public final String readLine() {
		final StringBuilder bld = new StringBuilder(32);

		int b;
		while ((b = readUnsignedByte()) != '\n') {
			if (b != '\r') {
				bld.append((char) b);
			}
		}

		if (bld.length() == 0) {
			return null;
		}

		// trim a trailing carriage return
		int len = bld.length();
		if (len > 0 && bld.charAt(len - 1) == '\r') {
			bld.setLength(len - 1);
		}
		return bld.toString();
	}

	@Override
	public final String readUTF() throws UTFDataFormatException {
		final int utflen = readUnsignedShort();

		final byte[] bytearr;
		final char[] chararr;

		if (this.utfByteBuffer == null || this.utfByteBuffer.length < utflen) {
			bytearr = new byte[utflen];
			this.utfByteBuffer = bytearr;
		} else {
			bytearr = this.utfByteBuffer;
		}
		if (this.utfCharBuffer == null || this.utfCharBuffer.length < utflen) {
			chararr = new char[utflen];
			this.utfCharBuffer = chararr;
		} else {
			chararr = this.utfCharBuffer;
		}

		int c, char2, char3;
		int count = 0;
		int chararrCount = 0;

		readFully(bytearr, 0, utflen);

		while (count < utflen) {
			c = (int) bytearr[count] & 0xff;
			if (c > 127) {
				break;
			}
			count++;
			chararr[chararrCount++] = (char) c;
		}

		while (count < utflen) {
			c = (int) bytearr[count] & 0xff;
			switch (c >> 4) {
			case 0:
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
				count++;
				chararr[chararrCount++] = (char) c;
				break;
			case 12:
			case 13:
				count += 2;
				if (count > utflen) {
					throw new UTFDataFormatException("malformed input: partial character at end");
				}
				char2 = bytearr[count - 1];
				if ((char2 & 0xC0) != 0x80) {
					throw new UTFDataFormatException("malformed input around byte " + count);
				}
				chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
				break;
			case 14:
				count += 3;
				if (count > utflen) {
					throw new UTFDataFormatException("malformed input: partial character at end");
				}
				char2 = bytearr[count - 2];
				char3 = bytearr[count - 1];
				if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
					throw new UTFDataFormatException("malformed input around byte " + (count - 1));
				}
				chararr[chararrCount++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
				break;
			default:
				throw new UTFDataFormatException("malformed input around byte " + count);
			}
		}
		// The number of chars produced may be less than utflen
		return new String(chararr, 0, chararrCount);
	}

	@Override
	public final int skipBytes(int n) {
		if (n < 0) {
			throw new IllegalArgumentException();
		}

		int toSkip = Math.min(n, remaining());
		this.position += toSkip;
		return toSkip;
	}

	@Override
	public void skipBytesToRead(int numBytes) throws EOFException {
		int skippedBytes = skipBytes(numBytes);

		if (skippedBytes < numBytes){
			throw new EOFException("Could not skip " + numBytes + " bytes.");
		}
	}

	@Override
	public int read(byte[] b, int off, int len) {
		if (b == null){
			throw new NullPointerException("Byte array b cannot be null.");
		}

		if (off < 0){
			throw new IllegalArgumentException("The offset off cannot be negative.");
		}

		if (len < 0){
			throw new IllegalArgumentException("The length len cannot be negative.");
		}

		int toRead = Math.min(len, remaining());
		this.segment.get(this.position, b, off, toRead);
		this.position += toRead;

		return toRead;
	}

	@Override
	public int read(byte[] b) {
		return read(b, 0, b.length);
	}
}
