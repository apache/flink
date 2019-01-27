/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.SynchronousBufferFileReader;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Readers for the data file produced by hash or merge file writer.
 */
public class BufferSortedDataFileReader<T> {
	private final SynchronousBufferFileReader synchronousStreamFileReader;
	private final RecordDeserializer<IOReadableWritable> recordDeserializer;
	private final DeserializationDelegate<T> deserializationDelegate;
	private final int segmentSize;

	/** The offset to start reading */
	private final long startOffset;

	/** The length in bytes to read */
	private final long length;

	private long remainingBytesToRead;
	private boolean eof;

	public BufferSortedDataFileReader(String path, String tmpDir, IOManager ioManager, int segmentSize,
									  TypeSerializer<T> serializer, long startOffset, long length) throws IOException {
		this.synchronousStreamFileReader = new SynchronousBufferFileReader(
			ioManager.createChannel(new File(path)), false, false);
		this.segmentSize = segmentSize;

		this.recordDeserializer = new SpillingAdaptiveSpanningRecordDeserializer<>(new String[]{tmpDir});
		this.deserializationDelegate = new ReusingDeserializationDelegate<>(serializer);

		this.startOffset = startOffset;
		this.length = length;
		this.remainingBytesToRead = length;

		synchronousStreamFileReader.seekToPosition(startOffset);
	}

	public T next() throws IOException {
		if (eof) {
			return null;
		}

		while (true) {
			RecordDeserializer.DeserializationResult deserializationResult = recordDeserializer.getNextRecord(deserializationDelegate);

			if (deserializationResult.isFullRecord()) {
				return deserializationDelegate.getInstance();
			}

			if (deserializationResult.isBufferConsumed()) {
				Buffer currentBuffer = recordDeserializer.getCurrentBuffer();
				if (currentBuffer != null) {
					currentBuffer.recycleBuffer();
				}

				if (remainingBytesToRead <= 0) {
					eof = true;
					return null;
				}

				checkState(!synchronousStreamFileReader.hasReachedEndOfFile(),
					"The file are fully read before allowed maximum buffers are read");

				Buffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(segmentSize),
					FreeingBufferRecycler.INSTANCE);
				long nextReadLength = Math.min(remainingBytesToRead, buffer.getMaxCapacity());
				synchronousStreamFileReader.readInto(buffer, nextReadLength);
				recordDeserializer.setNextBuffer(buffer);
				remainingBytesToRead -= nextReadLength;
			}
		}
	}

	public void close() {

	}
}
