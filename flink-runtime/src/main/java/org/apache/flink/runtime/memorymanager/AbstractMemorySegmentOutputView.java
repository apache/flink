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

package org.apache.flink.runtime.memorymanager;

import java.io.IOException;
import java.io.UTFDataFormatException;

import org.apache.commons.lang.Validate;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

/**
 * The base class for all output views that are backed by multiple memory pages. This base class contains all
 * encoding methods to write data to a page and detect page boundary crossing. The concrete sub classes must
 * implement {@link #advance()} for moving through the memory segments and the tell/seek related methods
 * from {@link org.apache.flink.core.memory.DataOutputView}.
 * <p>
 * The paging assumes that all memory segments are of the same size.
 */
public abstract class AbstractMemorySegmentOutputView implements DataOutputView {
	protected final int segmentSize;				// the size of the memory segments
	protected final int headerLength;				// the number of bytes to skip at the beginning of each segment
	protected MemorySegment currentSegment;			// the current memory segment to write to
	protected int positionInSegment;				// the offset in the current segment
	private byte[] utfBuffer;						// the reusable array for UTF encodings

	/**
	 * Creates a new output view that writes initially to the given initial segment. All segments in the
	 * view have to be of the given {@code segmentSize}. A header of length {@code headerLength} is left
	 * at the beginning of each segment.
	 *
	 * @param initialSegment The segment that the view starts writing to.
	 * @param segmentSize The size of the memory segments.
	 * @param headerLength The number of bytes to skip at the beginning of each segment for the header.
	 */
	protected AbstractMemorySegmentOutputView(MemorySegment initialSegment, int segmentSize, int headerLength) {
		Validate.notNull(initialSegment, "Initial Segment may not be null");
		this.segmentSize = segmentSize;
		this.headerLength = headerLength;
		this.currentSegment = initialSegment;
		this.positionInSegment = headerLength;
	}

	/**
	 * @param segmentSize The size of the memory segments.
	 * @param headerLength The number of bytes to skip at the beginning of each segment for the header.
	 */
	protected AbstractMemorySegmentOutputView(int segmentSize, int headerLength)
	{
		this.segmentSize = segmentSize;
		this.headerLength = headerLength;
		this.currentSegment = null;
		this.positionInSegment = headerLength;
	}

	/**
	 * Moves the output view to the next page. This must essentially update {@link #currentSegment} and {@link
	 * #positionInSegment}
	 */
	protected abstract void advance() throws IOException;

	@Override
	public void lock() {
		// Provide default methods since only the paging output views actually need to lock/unlock
	}

	@Override
	public void unlock() throws IOException {
		// Provide default methods since only the paging output views actually need to lock/unlock
	}

	// --------------------------------------------------------------------------------------------
	//                               Data Output Specific methods
	// --------------------------------------------------------------------------------------------
	@Override
	public void write(int b) throws IOException {
		writeByte(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		int remaining = this.segmentSize - this.positionInSegment;
		if (remaining >= len) {
			this.currentSegment.put(this.positionInSegment, b, off, len);
			this.positionInSegment += len;

		}
		else {
			if (remaining == 0) {
				advance();
				remaining = this.segmentSize - this.positionInSegment;
			}
			while (true) {
				int toPut = Math.min(remaining, len);
				this.currentSegment.put(this.positionInSegment, b, off, toPut);
				off += toPut;
				len -= toPut;

				if (len > 0) {
					this.positionInSegment = this.segmentSize;
					advance();
					remaining = this.segmentSize - this.positionInSegment;
				}
				else {
					this.positionInSegment += toPut;
					break;
				}
			}
		}
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		writeByte(v ? 1 : 0);
	}

	@Override
	public void writeByte(int v) throws IOException {
		if (this.positionInSegment < this.segmentSize) {
			this.currentSegment.put(this.positionInSegment++, (byte) v);
		}
		else {
			advance();
			writeByte(v);
		}
	}

	@Override
	public void writeShort(int v) throws IOException {
		if (this.positionInSegment < this.segmentSize - 1) {
			this.currentSegment.putShort(this.positionInSegment, (short) v);
			this.positionInSegment += 2;
		}
		else if (this.positionInSegment == this.segmentSize) {
			advance();
			writeShort(v);
		}
		else {
			writeByte(v >> 8);
			writeByte(v);
		}
	}

	@Override
	public void writeChar(int v) throws IOException {
		if (this.positionInSegment < this.segmentSize - 1) {
			this.currentSegment.putChar(this.positionInSegment, (char) v);
			this.positionInSegment += 2;
		}
		else if (this.positionInSegment == this.segmentSize) {
			advance();
			writeChar(v);
		}
		else {
			writeByte(v >> 8);
			writeByte(v);
		}
	}

	@Override
	public void writeInt(int v) throws IOException {
		if (this.positionInSegment < this.segmentSize - 3) {
			this.currentSegment.putIntBigEndian(this.positionInSegment, v);
			this.positionInSegment += 4;
		}
		else if (this.positionInSegment == this.segmentSize) {
			advance();
			writeInt(v);
		}
		else {
			writeByte(v >> 24);
			writeByte(v >> 16);
			writeByte(v >>  8);
			writeByte(v);
		}
	}

	@Override
	public void writeLong(long v) throws IOException {
		if (this.positionInSegment < this.segmentSize - 7) {
			this.currentSegment.putLongBigEndian(this.positionInSegment, v);
			this.positionInSegment += 8;
		}
		else if (this.positionInSegment == this.segmentSize) {
			advance();
			writeLong(v);
		}
		else {
			writeByte((int) (v >> 56));
			writeByte((int) (v >> 48));
			writeByte((int) (v >> 40));
			writeByte((int) (v >> 32));
			writeByte((int) (v >> 24));
			writeByte((int) (v >> 16));
			writeByte((int) (v >>  8));
			writeByte((int) v);
		}
	}

	@Override
	public void writeFloat(float v) throws IOException {
		writeInt(Float.floatToRawIntBits(v));
	}

	@Override
	public void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToRawLongBits(v));
	}

	@Override
	public void writeBytes(String s) throws IOException {
		for (int i = 0; i < s.length(); i++) {
			writeByte(s.charAt(i));
		}
	}

	@Override
	public void writeChars(String s) throws IOException {
		for (int i = 0; i < s.length(); i++) {
			writeChar(s.charAt(i));
		}
	}

	@Override
	public void writeUTF(String str) throws IOException {
		int strlen = str.length();
		int utflen = 0;
		int c, count = 0;

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
			throw new UTFDataFormatException("encoded string too long: " + utflen + " memory");
		}

		if (this.utfBuffer == null || this.utfBuffer.length < utflen + 2) {
			this.utfBuffer = new byte[utflen + 2];
		}
		final byte[] bytearr = this.utfBuffer;

		bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
		bytearr[count++] = (byte) ((utflen      ) & 0xFF);

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
				bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
				bytearr[count++] = (byte) (0x80 | ((c     ) & 0x3F));
			} else {
				bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
				bytearr[count++] = (byte) (0x80 | ((c     ) & 0x3F));
			}
		}

		write(bytearr, 0, utflen + 2);
	}

	@Override
	public void skipBytesToWrite(int numBytes) throws IOException {
		while (numBytes > 0) {
			final int remaining = this.segmentSize - this.positionInSegment;
			if (numBytes <= remaining) {
				this.positionInSegment += numBytes;
				return;
			}
			this.positionInSegment = this.segmentSize;
			advance();
			numBytes -= remaining;
		}
	}

	@Override
	public void write(DataInputView source, int numBytes) throws IOException {
		while (numBytes > 0) {
			final int remaining = this.segmentSize - this.positionInSegment;
			if (numBytes <= remaining) {
				this.currentSegment.put(source, this.positionInSegment, numBytes);
				this.positionInSegment += numBytes;
				return;
			}

			if (remaining > 0) {
				this.currentSegment.put(source, this.positionInSegment, remaining);
				this.positionInSegment = this.segmentSize;
				numBytes -= remaining;
			}

			advance();
		}
	}
}
