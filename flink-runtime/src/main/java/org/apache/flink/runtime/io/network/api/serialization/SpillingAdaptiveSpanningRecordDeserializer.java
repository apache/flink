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
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.FileUtils;
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
import java.util.Optional;
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
	public Optional<Buffer> getUnconsumedBuffer() throws IOException {
		Optional<MemorySegment> target;
		if (nonSpanningWrapper.remaining() > 0) {
			target = nonSpanningWrapper.getUnconsumedSegment();
		} else {
			target = spanningWrapper.getUnconsumedSegment();
		}
		return target.map(memorySegment -> new NetworkBuffer(
			memorySegment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, memorySegment.size()));
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

		Optional<MemorySegment> getUnconsumedSegment() throws IOException {
			// for the case of only partial length, no data
			final int position = lengthBuffer.position();
			if (position > 0) {
				MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(position);
				lengthBuffer.position(0);
				segment.put(0, lengthBuffer, position);
				return Optional.of(segment);
			}

			// for the case of full length, partial data in buffer
			if (recordLength > THRESHOLD_FOR_SPILLING) {
				throw new UnsupportedOperationException("Unaligned checkpoint currently do not support spilled " +
					"records.");
			} else if (recordLength != -1) {
				int leftOverSize = leftOverLimit - leftOverStart;
				int unconsumedSize = Integer.BYTES + accumulatedRecordBytes + leftOverSize;
				DataOutputSerializer serializer = new DataOutputSerializer(unconsumedSize);
				serializer.writeInt(recordLength);
				serializer.write(buffer, 0, accumulatedRecordBytes);
				if (leftOverData != null) {
					serializer.write(leftOverData, leftOverStart, leftOverSize);
				}
				MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(unconsumedSize);
				segment.put(0, serializer.getSharedBuffer(), 0, segment.size());
				return Optional.of(segment);
			}

			// for the case of no remaining partial length or data
			return Optional.empty();
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
			this.leftOverStart = 0;
			this.leftOverLimit = 0;
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
