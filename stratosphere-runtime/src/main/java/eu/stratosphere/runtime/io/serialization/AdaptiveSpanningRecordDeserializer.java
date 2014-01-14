/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.serialization;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.runtime.io.serialization.DataInputDeserializer;
import eu.stratosphere.runtime.io.serialization.DataOutputSerializer;
import eu.stratosphere.runtime.io.serialization.RecordDeserializer;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @param <T> The type of the record to be deserialized.
 */
public class AdaptiveSpanningRecordDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T> {
	
	private final NonSpanningWrapper nonSpanningWrapper;
	
	private final SpanningWrapper spanningWrapper;

	public AdaptiveSpanningRecordDeserializer() {
		this.nonSpanningWrapper = new NonSpanningWrapper();
		this.spanningWrapper = new SpanningWrapper();
	}
	
	@Override
	public void setNextMemorySegment(MemorySegment segment, int numBytes) throws IOException {
		// check if some spanning record deserialization is pending
		if (this.spanningWrapper.getNumGatheredBytes() > 0) {
			this.spanningWrapper.addNextChunkFromMemorySegment(segment, numBytes);
		}
		else {
			this.nonSpanningWrapper.initializeFromMemorySegment(segment, 0, numBytes);
		}
	}
	
	@Override
	public DeserializationResult getNextRecord(T target) throws IOException {
		// always check the non-spanning wrapper first.
		// this should be the majority of the cases for small records
		// for large records, this portion of the work is very small in comparison anyways
		
		int nonSpanningRemaining = this.nonSpanningWrapper.remaining();
		
		// check if we can get a full length;
		if (nonSpanningRemaining >= 4) {
			int len = this.nonSpanningWrapper.readInt();
			if (len <= nonSpanningRemaining - 4) {
				// we can get a full record from here
				target.read(this.nonSpanningWrapper);
				
				return (this.nonSpanningWrapper.remaining() == 0) ?
					DeserializationResult.LAST_RECORD_FROM_BUFFER :
					DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
			} else {
				// we got the length, but we need the rest from the spanning deserializer
				// and need to wait for more buffers
				this.spanningWrapper.initializeWithPartialRecord(this.nonSpanningWrapper, len);
				this.nonSpanningWrapper.clear();
				return DeserializationResult.PARTIAL_RECORD;
			}
		} else if (nonSpanningRemaining > 0) {
			// we have an incomplete length
			// add our part of the length to the length buffer
			this.spanningWrapper.initializeWithPartialLength(this.nonSpanningWrapper);
			this.nonSpanningWrapper.clear();
			return DeserializationResult.PARTIAL_RECORD;
		}
		
		// spanning record case
		if (this.spanningWrapper.hasFullRecord()) {
			// get the full record
			target.read(this.spanningWrapper);
			
			// move the remainder to the non-spanning wrapper
			// this does not copy it, only sets the memory segment
			this.spanningWrapper.moveRemainderToNonSpanningDeserializer(this.nonSpanningWrapper);
			this.spanningWrapper.clear();
			
			return (this.nonSpanningWrapper.remaining() == 0) ?
				DeserializationResult.LAST_RECORD_FROM_BUFFER :
				DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
//		} else if (this.spanningWrapper.getNumGatheredBytes() == 0) {
//			// error case. we are in the spanning deserializer, but it has no bytes, yet
//			throw new IllegalStateException();
		} else {
			return DeserializationResult.PARTIAL_RECORD;
		}
	}

	@Override
	public void clear() {
		this.nonSpanningWrapper.clear();
		this.spanningWrapper.clear();
	}

	@Override
	public boolean hasUnfinishedData() {
		return this.nonSpanningWrapper.remaining() > 0 || this.spanningWrapper.getNumGatheredBytes() > 0;
	}

	// -----------------------------------------------------------------------------------------------------------------
	
	private static final class NonSpanningWrapper implements DataInput {
		
		private MemorySegment segment;
		
		private int limit;
		
		private int position;
		
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
		
		// -------------------------------------------------------------------------------------------------------------
		//                                       DataInput specific methods
		// -------------------------------------------------------------------------------------------------------------
		
		@Override
		public final void readFully(byte[] b) throws IOException {
			readFully(b, 0, b.length);
		}

		@Override
		public final void readFully(byte[] b, int off, int len) throws IOException {
			if (off < 0 || len < 0 || off + len > b.length)
				throw new IndexOutOfBoundsException();
			
			this.segment.get(this.position, b, off, len);
			this.position += len;
		}

		@Override
		public final boolean readBoolean() throws IOException {
			return readByte() == 1;
		}

		@Override
		public final byte readByte() throws IOException {
			return this.segment.get(this.position++);
		}

		@Override
		public final int readUnsignedByte() throws IOException {
			return readByte() & 0xff;
		}

		@Override
		public final short readShort() throws IOException {
			final short v = this.segment.getShort(this.position);
			this.position += 2;
			return v;
		}

		@Override
		public final int readUnsignedShort() throws IOException {
			final int v = this.segment.getShort(this.position) & 0xffff;
			this.position += 2;
			return v;
		}

		@Override
		public final char readChar() throws IOException  {
			final char v = this.segment.getChar(this.position);
			this.position += 2;
			return v;
		}

		@Override
		public final int readInt() throws IOException {
			final int v = this.segment.getIntBigEndian(this.position);
			this.position += 4;
			return v;
		}

		@Override
		public final long readLong() throws IOException {
			final long v = this.segment.getLongBigEndian(this.position);
			this.position += 8;
			return v;
		}

		@Override
		public final float readFloat() throws IOException {
			return Float.intBitsToFloat(readInt());
		}

		@Override
		public final double readDouble() throws IOException {
			return Double.longBitsToDouble(readLong());
		}

		@Override
		public final String readLine() throws IOException {
			final StringBuilder bld = new StringBuilder(32);
			
			try {
				int b;
				while ((b = readUnsignedByte()) != '\n') {
					if (b != '\r')
						bld.append((char) b);
				}
			}
			catch (EOFException eofex) {}

			if (bld.length() == 0)
				return null;
			
			// trim a trailing carriage return
			int len = bld.length();
			if (len > 0 && bld.charAt(len - 1) == '\r') {
				bld.setLength(len - 1);
			}
			return bld.toString();
		}

		@Override
		public final String readUTF() throws IOException {
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
			int chararr_count = 0;

			readFully(bytearr, 0, utflen);

			while (count < utflen) {
				c = (int) bytearr[count] & 0xff;
				if (c > 127)
					break;
				count++;
				chararr[chararr_count++] = (char) c;
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
					chararr[chararr_count++] = (char) c;
					break;
				case 12:
				case 13:
					count += 2;
					if (count > utflen)
						throw new UTFDataFormatException("malformed input: partial character at end");
					char2 = (int) bytearr[count - 1];
					if ((char2 & 0xC0) != 0x80)
						throw new UTFDataFormatException("malformed input around byte " + count);
					chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
					break;
				case 14:
					count += 3;
					if (count > utflen)
						throw new UTFDataFormatException("malformed input: partial character at end");
					char2 = (int) bytearr[count - 2];
					char3 = (int) bytearr[count - 1];
					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
						throw new UTFDataFormatException("malformed input around byte " + (count - 1));
					chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
					break;
				default:
					throw new UTFDataFormatException("malformed input around byte " + count);
				}
			}
			// The number of chars produced may be less than utflen
			return new String(chararr, 0, chararr_count);
		}
		
		@Override
		public final int skipBytes(int n) throws IOException {
			if (n < 0)
				throw new IllegalArgumentException();
			
			int toSkip = Math.min(n, remaining());
			this.position += toSkip;
			return toSkip;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	
	private static final class SpanningWrapper implements DataInput {

		private final DataOutputSerializer serializationBuffer;

		private final DataInputDeserializer serializationReadBuffer;

		private final ByteBuffer lengthBuffer;

		private int recordLength;

		private MemorySegment leftOverData;

		private int leftOverStart;

		private int leftOverLimit;

		private int recordLimit;

		public SpanningWrapper() {
			this.lengthBuffer = ByteBuffer.allocate(4);
			this.lengthBuffer.order(ByteOrder.BIG_ENDIAN);

			this.recordLength = -1;

			this.serializationBuffer = new DataOutputSerializer(1024);
			this.serializationReadBuffer = new DataInputDeserializer();
		}
		
		private void initializeWithPartialRecord(NonSpanningWrapper partial, int nextRecordLength) throws IOException {
			// set the length and copy what is available to the buffer
			this.recordLength = nextRecordLength;
			this.recordLimit = partial.remaining();
			partial.segment.get(this.serializationBuffer, partial.position, partial.remaining());
			this.serializationReadBuffer.setBuffer(this.serializationBuffer.wrapAsByteBuffer());
		}
		
		private void initializeWithPartialLength(NonSpanningWrapper partial) throws IOException {
			// copy what we have to the length buffer
			partial.segment.get(partial.position, this.lengthBuffer, partial.remaining());
		}
		
		private void addNextChunkFromMemorySegment(MemorySegment segment, int numBytesInSegment) throws IOException {
			int segmentPosition = 0;
			
			// check where to go. if we have a partial length, we need to complete it first
			if (this.lengthBuffer.position() > 0) {
				int toPut = Math.min(this.lengthBuffer.remaining(), numBytesInSegment);
				segment.get(0, this.lengthBuffer, toPut);
				
				// did we complete the length?
				if (this.lengthBuffer.hasRemaining()) {
					return;
				} else {
					this.recordLength = this.lengthBuffer.getInt(0);
					this.lengthBuffer.clear();
					segmentPosition = toPut;
				}
			}

			// copy as much as we need or can for this next spanning record
			int needed = this.recordLength - this.recordLimit;
			int available = numBytesInSegment - segmentPosition;
			int toCopy = Math.min(needed, available);

			segment.get(this.serializationBuffer, segmentPosition, toCopy);
			this.recordLimit += toCopy;
			
			if (toCopy < available) {
				// there is more data in the segment
				this.leftOverData = segment;
				this.leftOverStart = segmentPosition + toCopy;
				this.leftOverLimit = numBytesInSegment;
			}

			// update read view
			this.serializationReadBuffer.setBuffer(this.serializationBuffer.wrapAsByteBuffer());
		}
		
		private void moveRemainderToNonSpanningDeserializer(NonSpanningWrapper deserializer) {
			deserializer.clear();
			
			if (leftOverData != null) {
				deserializer.initializeFromMemorySegment(leftOverData, leftOverStart, leftOverLimit);
			}
		}
		
		private boolean hasFullRecord() {
			return this.recordLength >= 0 && this.recordLimit >= this.recordLength;
		}
		
		private int getNumGatheredBytes() {
			return this.recordLimit + (this.recordLength >= 0 ? 4 : lengthBuffer.position()) + this.serializationBuffer.length();
		}

		public void clear() {
			this.serializationBuffer.clear();

			this.recordLength = -1;
			this.lengthBuffer.clear();
			this.leftOverData = null;
			this.recordLimit = 0;
		}

		// -------------------------------------------------------------------------------------------------------------
		//                                       DataInput specific methods
		// -------------------------------------------------------------------------------------------------------------

		@Override
		public void readFully(byte[] b) throws IOException {
			this.serializationReadBuffer.readFully(b);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			this.serializationReadBuffer.readFully(b, off, len);
		}

		@Override
		public int skipBytes(int n) throws IOException {
			return this.serializationReadBuffer.skipBytes(n);
		}

		@Override
		public boolean readBoolean() throws IOException {
			return this.serializationReadBuffer.readBoolean();
		}

		@Override
		public byte readByte() throws IOException {
			return this.serializationReadBuffer.readByte();
		}

		@Override
		public int readUnsignedByte() throws IOException {
			return this.serializationReadBuffer.readUnsignedByte();
		}

		@Override
		public short readShort() throws IOException {
			return this.serializationReadBuffer.readShort();
		}

		@Override
		public int readUnsignedShort() throws IOException {
			return this.serializationReadBuffer.readUnsignedShort();
		}

		@Override
		public char readChar() throws IOException {
			return this.serializationReadBuffer.readChar();
		}

		@Override
		public int readInt() throws IOException {
			return this.serializationReadBuffer.readInt();
		}

		@Override
		public long readLong() throws IOException {
			return this.serializationReadBuffer.readLong();
		}

		@Override
		public float readFloat() throws IOException {
			return this.serializationReadBuffer.readFloat();
		}

		@Override
		public double readDouble() throws IOException {
			return this.serializationReadBuffer.readDouble();
		}

		@Override
		public String readLine() throws IOException {
			return this.serializationReadBuffer.readLine();
		}

		@Override
		public String readUTF() throws IOException {
			return this.serializationReadBuffer.readUTF();
		}
	}
}
