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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.ID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.EnvironmentInformation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An {@link IOManager} that cannot do I/O but serves as a mock for tests.
 */
public class NoOpIOManager extends IOManager {

	public NoOpIOManager() {
		super(new String[] {EnvironmentInformation.getTemporaryFileDirectory()});
	}

	@Override
	public BlockChannelWriter<MemorySegment> createBlockChannelWriter(ID channelID, LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BlockChannelWriterWithCallback<MemorySegment> createBlockChannelWriter(ID channelID, RequestDoneCallback<MemorySegment> callback) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BlockChannelReader<MemorySegment> createBlockChannelReader(ID channelID, LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BufferFileWriter createBufferFileWriter(ID channelID) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BufferFileReader createBufferFileReader(ID channelID, RequestDoneCallback<Buffer> callback) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BufferFileSegmentReader createBufferFileSegmentReader(ID channelID, RequestDoneCallback<FileSegment> callback) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BulkBlockChannelReader createBulkBlockChannelReader(ID channelID, List<MemorySegment> targetSegments, int numBlocks) throws IOException {
		throw new UnsupportedOperationException();
	}
}
