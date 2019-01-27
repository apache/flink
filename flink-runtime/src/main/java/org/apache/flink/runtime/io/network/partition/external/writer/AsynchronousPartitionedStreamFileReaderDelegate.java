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

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An utility class helps read buffers with limited free memory segments from
 * a buffer file.
 *
 * <p>It first sends requests as long as there are available buffers. Then after a
 * buffer is returned, it will then send a new request with the recycled buffers.
 */
public class AsynchronousPartitionedStreamFileReaderDelegate implements RequestDoneCallback<Buffer>, BufferRecycler {
	private final BufferFileReader reader;
	private final Queue<MemorySegment> freeSegments;
	private final LinkedBlockingQueue<Buffer> retBuffers = new LinkedBlockingQueue<>();
	private final List<PartitionIndex> partitionIndices;

	private int nextPartitionIdx;
	private long nextOffset;

	private final AtomicReference<IOException> cause = new AtomicReference<>(null);

	public AsynchronousPartitionedStreamFileReaderDelegate(IOManager ioManager, FileIOChannel.ID channel,
														   List<MemorySegment> segments,
														   List<PartitionIndex> partitionIndices)throws IOException {

		this.reader = ioManager.createStreamFileReader(channel, this);
		this.freeSegments = new ArrayDeque<>(segments);
		this.partitionIndices = partitionIndices;

		MemorySegment segment;
		while ((segment = freeSegments.poll()) != null) {
			sendRequestIfFeasible(segment);
		}
	}

	BufferFileReader getReader() {
		return reader;
	}

	Buffer getNextBufferBlocking() throws InterruptedException, IOException {
		Buffer buffer;
		do {
			if (cause.get() != null) {
				throw cause.get();
			}
			buffer = retBuffers.poll(500, TimeUnit.MILLISECONDS);
		} while (buffer == null);

		return buffer;
	}

	private void sendRequestIfFeasible(MemorySegment memorySegment) throws IOException {
		long nextReadLength = 0;
		while (nextPartitionIdx < partitionIndices.size()) {
			PartitionIndex partitionIndex = partitionIndices.get(nextPartitionIdx);
			long partitionEndOffset = partitionIndex.getStartOffset() + partitionIndex.getLength();
			assert partitionEndOffset >= nextOffset;
			if (partitionEndOffset > nextOffset) {
				nextReadLength = Math.min(partitionEndOffset - nextOffset, memorySegment.size());
				break;
			}
			nextPartitionIdx++;
		}
		if (nextReadLength > 0) {
			Buffer buffer = new NetworkBuffer(memorySegment, this);
			reader.readInto(buffer, nextReadLength);
			nextOffset += nextReadLength;
		}
	}

	@Override
	public void requestSuccessful(Buffer buffer) {
		retBuffers.add(buffer);
	}

	@Override
	public void requestFailed(Buffer buffer, IOException e) {
		cause.compareAndSet(null, e);
		throw new RuntimeException(e);
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		try {
			sendRequestIfFeasible(memorySegment);
		} catch (IOException e) {
			cause.compareAndSet(null, e);
			throw new RuntimeException(e);
		}
	}

	void close() throws IOException {
		this.reader.close();
		this.freeSegments.clear();
	}
}
