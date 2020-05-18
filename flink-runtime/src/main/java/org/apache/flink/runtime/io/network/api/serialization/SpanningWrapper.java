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

import org.apache.flink.core.fs.RefCountedFile;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.flink.runtime.io.network.api.serialization.NonSpanningWrapper.singleBufferIterator;
import static org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer.LENGTH_BYTES;
import static org.apache.flink.util.FileUtils.writeCompletely;
import static org.apache.flink.util.IOUtils.closeAllQuietly;

final class SpanningWrapper {

	private static final int THRESHOLD_FOR_SPILLING = 5 * 1024 * 1024; // 5 MiBytes
	private static final int FILE_BUFFER_SIZE = 2 * 1024 * 1024;

	private final byte[] initialBuffer = new byte[1024];

	private final String[] tempDirs;

	private final Random rnd = new Random();

	private final DataInputDeserializer serializationReadBuffer;

	final ByteBuffer lengthBuffer;

	private FileChannel spillingChannel;

	private byte[] buffer;

	private int recordLength;

	private int accumulatedRecordBytes;

	private MemorySegment leftOverData;

	private int leftOverStart;

	private int leftOverLimit;

	private RefCountedFile spillFile;

	private DataInputViewStreamWrapper spillFileReader;

	SpanningWrapper(String[] tempDirs) {
		this.tempDirs = tempDirs;

		this.lengthBuffer = ByteBuffer.allocate(LENGTH_BYTES);
		this.lengthBuffer.order(ByteOrder.BIG_ENDIAN);

		this.recordLength = -1;

		this.serializationReadBuffer = new DataInputDeserializer();
		this.buffer = initialBuffer;
	}

	/**
	 * Copies the data and transfers the "ownership" (i.e. clears the passed wrapper).
	 */
	void transferFrom(NonSpanningWrapper partial, int nextRecordLength) throws IOException {
		updateLength(nextRecordLength);
		accumulatedRecordBytes = isAboveSpillingThreshold() ? spill(partial) : partial.copyContentTo(buffer);
		partial.clear();
	}

	private boolean isAboveSpillingThreshold() {
		return recordLength > THRESHOLD_FOR_SPILLING;
	}

	void addNextChunkFromMemorySegment(MemorySegment segment, int offset, int numBytes) throws IOException {
		int limit = offset + numBytes;
		int numBytesRead = isReadingLength() ? readLength(segment, offset, numBytes) : 0;
		offset += numBytesRead;
		numBytes -= numBytesRead;
		if (numBytes == 0) {
			return;
		}

		int toCopy = min(recordLength - accumulatedRecordBytes, numBytes);
		if (toCopy > 0) {
			copyFromSegment(segment, offset, toCopy);
		}
		if (numBytes > toCopy) {
			leftOverData = segment;
			leftOverStart = offset + toCopy;
			leftOverLimit = limit;
		}
	}

	private void copyFromSegment(MemorySegment segment, int offset, int length) throws IOException {
		if (spillingChannel == null) {
			copyIntoBuffer(segment, offset, length);
		} else {
			copyIntoFile(segment, offset, length);
		}
	}

	private void copyIntoFile(MemorySegment segment, int offset, int length) throws IOException {
		writeCompletely(spillingChannel, segment.wrap(offset, length));
		accumulatedRecordBytes += length;
		if (hasFullRecord()) {
			spillingChannel.close();
			spillFileReader = new DataInputViewStreamWrapper(new BufferedInputStream(new FileInputStream(spillFile.getFile()), FILE_BUFFER_SIZE));
		}
	}

	private void copyIntoBuffer(MemorySegment segment, int offset, int length) {
		segment.get(offset, buffer, accumulatedRecordBytes, length);
		accumulatedRecordBytes += length;
		if (hasFullRecord()) {
			serializationReadBuffer.setBuffer(buffer, 0, recordLength);
		}
	}

	private int readLength(MemorySegment segment, int segmentPosition, int segmentRemaining) throws IOException {
		int bytesToRead = min(lengthBuffer.remaining(), segmentRemaining);
		segment.get(segmentPosition, lengthBuffer, bytesToRead);
		if (!lengthBuffer.hasRemaining()) {
			updateLength(lengthBuffer.getInt(0));
		}
		return bytesToRead;
	}

	private void updateLength(int length) throws IOException {
		lengthBuffer.clear();
		recordLength = length;
		if (isAboveSpillingThreshold()) {
			spillingChannel = createSpillingChannel();
		} else {
			ensureBufferCapacity(length);
		}
	}

	CloseableIterator<Buffer> getUnconsumedSegment() throws IOException {
		if (isReadingLength()) {
			return singleBufferIterator(copyLengthBuffer());
		} else if (isAboveSpillingThreshold()) {
			throw new UnsupportedOperationException("Unaligned checkpoint currently do not support spilled records.");
		} else if (recordLength == -1) {
			return CloseableIterator.empty(); // no remaining partial length or data
		} else {
			return singleBufferIterator(copyDataBuffer());
		}
	}

	private MemorySegment copyLengthBuffer() {
		int position = lengthBuffer.position();
		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(position);
		lengthBuffer.position(0);
		segment.put(0, lengthBuffer, position);
		return segment;
	}

	private MemorySegment copyDataBuffer() throws IOException {
		int leftOverSize = leftOverLimit - leftOverStart;
		int unconsumedSize = LENGTH_BYTES + accumulatedRecordBytes + leftOverSize;
		DataOutputSerializer serializer = new DataOutputSerializer(unconsumedSize);
		serializer.writeInt(recordLength);
		serializer.write(buffer, 0, accumulatedRecordBytes);
		if (leftOverData != null) {
			serializer.write(leftOverData, leftOverStart, leftOverSize);
		}
		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(unconsumedSize);
		segment.put(0, serializer.getSharedBuffer(), 0, segment.size());
		return segment;
	}

	/**
	 * Copies the leftover data and transfers the "ownership" (i.e. clears this wrapper).
	 */
	void transferLeftOverTo(NonSpanningWrapper nonSpanningWrapper) {
		nonSpanningWrapper.clear();
		if (leftOverData != null) {
			nonSpanningWrapper.initializeFromMemorySegment(leftOverData, leftOverStart, leftOverLimit);
		}
		clear();
	}

	boolean hasFullRecord() {
		return recordLength >= 0 && accumulatedRecordBytes >= recordLength;
	}

	int getNumGatheredBytes() {
		return accumulatedRecordBytes + (recordLength >= 0 ? LENGTH_BYTES : lengthBuffer.position());
	}

	public void clear() {
		buffer = initialBuffer;
		serializationReadBuffer.releaseArrays();

		recordLength = -1;
		lengthBuffer.clear();
		leftOverData = null;
		leftOverStart = 0;
		leftOverLimit = 0;
		accumulatedRecordBytes = 0;

		closeAllQuietly(spillingChannel, spillFileReader, () -> spillFile.release());
		spillingChannel = null;
		spillFileReader = null;
		spillFile = null;
	}

	public DataInputView getInputView() {
		return spillFileReader == null ? serializationReadBuffer : spillFileReader;
	}

	private void ensureBufferCapacity(int minLength) {
		if (buffer.length < minLength) {
			byte[] newBuffer = new byte[max(minLength, buffer.length * 2)];
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
			File file = new File(directory, randomString(rnd) + ".inputchannel");
			if (file.createNewFile()) {
				spillFile = new RefCountedFile(file);
				return new RandomAccessFile(file, "rw").getChannel();
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

	private int spill(NonSpanningWrapper partial) throws IOException {
		ByteBuffer buffer = partial.wrapIntoByteBuffer();
		int length = buffer.remaining();
		writeCompletely(spillingChannel, buffer);
		return length;
	}

	private boolean isReadingLength() {
		return lengthBuffer.position() > 0;
	}

}
