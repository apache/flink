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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

/**
 * @param <T> The type of the record to be deserialized.
 */
public class SpillingAdaptiveSpanningRecordDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T> {

	private static final String BROKEN_SERIALIZATION_ERROR_MESSAGE =
					"Serializer consumed more bytes than the record had. " +
					"This indicates broken serialization. If you are using custom serialization types " +
					"(Value or Writable), check their serialization methods. If you are using a " +
					"Kryo-serialized type, check the corresponding Kryo serializer.";

	private static final int THRESHOLD_FOR_SPILLING = 5 * 1024 * 1024; // 5 MiBytes

	private final NonSpanningWrapper nonSpanningWrapper;

	private final SpanningWrapper spanningWrapper;

	private Buffer currentBuffer;

	public SpillingAdaptiveSpanningRecordDeserializer(String[] tmpDirectories) {
		this.nonSpanningWrapper = new NonSpanningWrapper();
		this.spanningWrapper = new SpanningWrapper(tmpDirectories);
	}

	@Override
	public void setNextBuffer(Buffer buffer) throws IOException {
		currentBuffer = buffer;

		int offset = buffer.getMemorySegmentOffset();
		MemorySegment segment = buffer.getMemorySegment();
		int numBytes = buffer.getSize();

		// check if some spanning record deserialization is pending
		if (this.spanningWrapper.getNumGatheredBytes() > 0) {
			this.spanningWrapper.addNextChunkFromMemorySegment(segment, offset, numBytes);
		}
		else {
			this.nonSpanningWrapper.initializeFromMemorySegment(segment, offset, numBytes + offset);
		}
	}

	@Override
	public Buffer getCurrentBuffer () {
		Buffer tmp = currentBuffer;
		currentBuffer = null;
		return tmp;
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
				try {
					target.read(this.nonSpanningWrapper);

					int remaining = this.nonSpanningWrapper.remaining();
					if (remaining > 0) {
						return DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
					}
					else if (remaining == 0) {
						return DeserializationResult.LAST_RECORD_FROM_BUFFER;
					}
					else {
						throw new IndexOutOfBoundsException("Remaining = " + remaining);
					}
				}
				catch (IndexOutOfBoundsException e) {
					throw new IOException(BROKEN_SERIALIZATION_ERROR_MESSAGE, e);
				}
			}
			else {
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
			target.read(this.spanningWrapper.getInputView());

			// move the remainder to the non-spanning wrapper
			// this does not copy it, only sets the memory segment
			this.spanningWrapper.moveRemainderToNonSpanningDeserializer(this.nonSpanningWrapper);
			this.spanningWrapper.clear();

			return (this.nonSpanningWrapper.remaining() == 0) ?
				DeserializationResult.LAST_RECORD_FROM_BUFFER :
				DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
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

	private static final class NonSpanningWrapper implements DataInputView {

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
			if (off < 0 || len < 0 || off + len > b.length) {
				throw new IndexOutOfBoundsException();
			}

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
			final short v = this.segment.getShortBigEndian(this.position);
			this.position += 2;
			return v;
		}

		@Override
		public final int readUnsignedShort() throws IOException {
			final int v = this.segment.getShortBigEndian(this.position) & 0xffff;
			this.position += 2;
			return v;
		}

		@Override
		public final char readChar() throws IOException  {
			final char v = this.segment.getCharBigEndian(this.position);
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
					if (b != '\r') {
						bld.append((char) b);
					}
				}
			}
			catch (EOFException ignored) {}

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
					char2 = (int) bytearr[count - 1];
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
					char2 = (int) bytearr[count - 2];
					char3 = (int) bytearr[count - 1];
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
		public final int skipBytes(int n) throws IOException {
			if (n < 0) {
				throw new IllegalArgumentException();
			}

			int toSkip = Math.min(n, remaining());
			this.position += toSkip;
			return toSkip;
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			int skippedBytes = skipBytes(numBytes);

			if (skippedBytes < numBytes){
				throw new EOFException("Could not skip " + numBytes + " bytes.");
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
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
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	private static final class SpanningWrapper {

		private final byte[] initialBuffer = new byte[1024];

		private final String[] tempDirs;

		private final Random rnd = new Random();

		private final DataInputDeserializer serializationReadBuffer;

		private final ByteBuffer lengthBuffer;

		private FileChannel spillingChannel;

		private byte[] buffer;

		private int recordLength;

		private int accumulatedRecordBytes;

		private MemorySegment leftOverData;

		private int leftOverStart;

		private int leftOverLimit;

		private File spillFile;

		private DataInputViewStreamWrapper spillFileReader;

		public SpanningWrapper(String[] tempDirs) {
			this.tempDirs = tempDirs;

			this.lengthBuffer = ByteBuffer.allocate(4);
			this.lengthBuffer.order(ByteOrder.BIG_ENDIAN);

			this.recordLength = -1;

			this.serializationReadBuffer = new DataInputDeserializer();
			this.buffer = initialBuffer;
		}

		private void initializeWithPartialRecord(NonSpanningWrapper partial, int nextRecordLength) throws IOException {
			// set the length and copy what is available to the buffer
			this.recordLength = nextRecordLength;

			final int numBytesChunk = partial.remaining();

			if (nextRecordLength > THRESHOLD_FOR_SPILLING) {
				// create a spilling channel and put the data there
				this.spillingChannel = createSpillingChannel();

				ByteBuffer toWrite = partial.segment.wrap(partial.position, numBytesChunk);
				FileUtils.writeCompletely(this.spillingChannel, toWrite);
			}
			else {
				// collect in memory
				ensureBufferCapacity(nextRecordLength);
				partial.segment.get(partial.position, buffer, 0, numBytesChunk);
			}

			this.accumulatedRecordBytes = numBytesChunk;
		}

		private void initializeWithPartialLength(NonSpanningWrapper partial) throws IOException {
			// copy what we have to the length buffer
			partial.segment.get(partial.position, this.lengthBuffer, partial.remaining());
		}

		private void addNextChunkFromMemorySegment(MemorySegment segment, int offset, int numBytes) throws IOException {
			int segmentPosition = offset;
			int segmentRemaining = numBytes;
			// check where to go. if we have a partial length, we need to complete it first
			if (this.lengthBuffer.position() > 0) {
				int toPut = Math.min(this.lengthBuffer.remaining(), segmentRemaining);
				segment.get(segmentPosition, this.lengthBuffer, toPut);
				// did we complete the length?
				if (this.lengthBuffer.hasRemaining()) {
					return;
				} else {
					this.recordLength = this.lengthBuffer.getInt(0);

					this.lengthBuffer.clear();
					segmentPosition += toPut;
					segmentRemaining -= toPut;
					if (this.recordLength > THRESHOLD_FOR_SPILLING) {
						this.spillingChannel = createSpillingChannel();
					} else {
						ensureBufferCapacity(this.recordLength);
					}
				}
			}

			// copy as much as we need or can for this next spanning record
			int needed = this.recordLength - this.accumulatedRecordBytes;
			int toCopy = Math.min(needed, segmentRemaining);

			if (spillingChannel != null) {
				// spill to file
				ByteBuffer toWrite = segment.wrap(segmentPosition, toCopy);
				FileUtils.writeCompletely(this.spillingChannel, toWrite);
			} else {
				segment.get(segmentPosition, buffer, this.accumulatedRecordBytes, toCopy);
			}

			this.accumulatedRecordBytes += toCopy;

			if (toCopy < segmentRemaining) {
				// there is more data in the segment
				this.leftOverData = segment;
				this.leftOverStart = segmentPosition + toCopy;
				this.leftOverLimit = numBytes + offset;
			}

			if (accumulatedRecordBytes == recordLength) {
				// we have the full record
				if (spillingChannel == null) {
					this.serializationReadBuffer.setBuffer(buffer, 0, recordLength);
				}
				else {
					spillingChannel.close();

					BufferedInputStream inStream = new BufferedInputStream(new FileInputStream(spillFile), 2 * 1024 * 1024);
					this.spillFileReader = new DataInputViewStreamWrapper(inStream);
				}
			}
		}

		private void moveRemainderToNonSpanningDeserializer(NonSpanningWrapper deserializer) {
			deserializer.clear();

			if (leftOverData != null) {
				deserializer.initializeFromMemorySegment(leftOverData, leftOverStart, leftOverLimit);
			}
		}

		private boolean hasFullRecord() {
			return this.recordLength >= 0 && this.accumulatedRecordBytes >= this.recordLength;
		}

		private int getNumGatheredBytes() {
			return this.accumulatedRecordBytes + (this.recordLength >= 0 ? 4 : lengthBuffer.position());
		}

		public void clear() {
			this.buffer = initialBuffer;
			this.serializationReadBuffer.releaseArrays();

			this.recordLength = -1;
			this.lengthBuffer.clear();
			this.leftOverData = null;
			this.accumulatedRecordBytes = 0;

			if (spillingChannel != null) {
				try {
					spillingChannel.close();
				}
				catch (Throwable t) {
					// ignore
				}
				spillingChannel = null;
			}
			if (spillFileReader != null) {
				try {
					spillFileReader.close();
				}
				catch (Throwable t) {
					// ignore
				}
				spillFileReader = null;
			}
			if (spillFile != null) {
				spillFile.delete();
				spillFile = null;
			}
		}

		public DataInputView getInputView() {
			if (spillFileReader == null) {
				return serializationReadBuffer;
			}
			else {
				return spillFileReader;
			}
		}

		private void ensureBufferCapacity(int minLength) {
			if (buffer.length < minLength) {
				byte[] newBuffer = new byte[Math.max(minLength, buffer.length * 2)];
				System.arraycopy(buffer, 0, newBuffer, 0, accumulatedRecordBytes);
				buffer = newBuffer;
			}
		}

		@SuppressWarnings("resource")
		private FileChannel createSpillingChannel() throws IOException {
			if (spillFile != null) {
				throw new IllegalStateException("Spilling file already exists.");
			}

			// try to find a unique file name for the spilling channel
			int maxAttempts = 10;
			for (int attempt = 0; attempt < maxAttempts; attempt++) {
				String directory = tempDirs[rnd.nextInt(tempDirs.length)];
				spillFile = new File(directory, randomString(rnd) + ".inputchannel");
				if (spillFile.createNewFile()) {
					return new RandomAccessFile(spillFile, "rw").getChannel();
				}
			}

			throw new IOException(
				"Could not find a unique file channel name in '" + Arrays.toString(tempDirs) +
					"' for spilling large records during deserialization.");
		}

		private static String randomString(Random random) {
			final byte[] bytes = new byte[20];
			random.nextBytes(bytes);
			return StringUtils.byteToHexString(bytes);
		}
	}
}
