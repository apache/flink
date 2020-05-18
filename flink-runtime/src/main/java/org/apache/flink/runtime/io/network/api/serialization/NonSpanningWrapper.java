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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer.NextRecordResponse;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.CloseableIterator;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.PARTIAL_RECORD;
import static org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer.LENGTH_BYTES;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;

final class NonSpanningWrapper implements DataInputView {

	private static final String BROKEN_SERIALIZATION_ERROR_MESSAGE =
			"Serializer consumed more bytes than the record had. " +
					"This indicates broken serialization. If you are using custom serialization types " +
					"(Value or Writable), check their serialization methods. If you are using a " +
					"Kryo-serialized type, check the corresponding Kryo serializer.";

	private MemorySegment segment;

	private int limit;

	private int position;

	private byte[] utfByteBuffer; // reusable byte buffer for utf-8 decoding
	private char[] utfCharBuffer; // reusable char buffer for utf-8 decoding

	private final NextRecordResponse reusedNextRecordResponse = new NextRecordResponse(null, 0); // performance impact of immutable objects not benchmarked

	private int remaining() {
		return this.limit - this.position;
	}

	void clear() {
		this.segment = null;
		this.limit = 0;
		this.position = 0;
	}

	void initializeFromMemorySegment(MemorySegment seg, int position, int limit) {
		this.segment = seg;
		this.position = position;
		this.limit = limit;
	}

	CloseableIterator<Buffer> getUnconsumedSegment() {
		if (!hasRemaining()) {
			return CloseableIterator.empty();
		}
		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(remaining());
		this.segment.copyTo(position, segment, 0, remaining());
		return singleBufferIterator(segment);
	}

	boolean hasRemaining() {
		return remaining() > 0;
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

	ByteBuffer wrapIntoByteBuffer() {
		return segment.wrap(position, remaining());
	}

	int copyContentTo(byte[] dst) {
		final int numBytesChunk = remaining();
		segment.get(position, dst, 0, numBytesChunk);
		return numBytesChunk;
	}

	/**
	 * Copies the data and transfers the "ownership" (i.e. clears current wrapper).
	 */
	void transferTo(ByteBuffer dst) {
		segment.get(position, dst, remaining());
		clear();
	}

	NextRecordResponse getNextRecord(IOReadableWritable target) throws IOException {
		int recordLen = readInt();
		if (canReadRecord(recordLen)) {
			return readInto(target);
		} else {
			return reusedNextRecordResponse.updated(PARTIAL_RECORD, recordLen);
		}
	}

	private NextRecordResponse readInto(IOReadableWritable target) throws IOException {
		try {
			target.read(this);
		} catch (IndexOutOfBoundsException e) {
			throw new IOException(BROKEN_SERIALIZATION_ERROR_MESSAGE, e);
		}
		int remaining = remaining();
		if (remaining < 0) {
			throw new IOException(BROKEN_SERIALIZATION_ERROR_MESSAGE, new IndexOutOfBoundsException("Remaining = " + remaining));
		}
		return reusedNextRecordResponse.updated(remaining == 0 ? LAST_RECORD_FROM_BUFFER : INTERMEDIATE_RECORD_FROM_BUFFER, remaining);
	}

	boolean hasCompleteLength() {
		return remaining() >= LENGTH_BYTES;
	}

	private boolean canReadRecord(int recordLength) {
		return recordLength <= remaining();
	}

	static CloseableIterator<Buffer> singleBufferIterator(MemorySegment target) {
		return CloseableIterator.ofElement(
			new NetworkBuffer(target, FreeingBufferRecycler.INSTANCE, DATA_BUFFER, target.size()),
			Buffer::recycleBuffer);
	}

}
