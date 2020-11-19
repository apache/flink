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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.RefCountedFile;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.CloseableIterator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.flink.core.memory.MemorySegmentFactory.wrap;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link CloseableIterator} of {@link Buffer buffers} over file content.
 */
@Internal
public class FileBasedBufferIterator implements CloseableIterator<Buffer> {

	private final RefCountedFile file;
	private final FileInputStream stream;
	private final int bufferSize;

	private int offset;
	private int bytesToRead;

	public FileBasedBufferIterator(RefCountedFile file, int bytesToRead, int bufferSize) throws FileNotFoundException {
		checkNotNull(file);
		checkArgument(bytesToRead >= 0);
		checkArgument(bufferSize > 0);
		this.stream = new FileInputStream(file.getFile());
		this.file = file;
		this.bufferSize = bufferSize;
		this.bytesToRead = bytesToRead;
		file.retain();
	}

	@Override
	public boolean hasNext() {
		return bytesToRead > 0;
	}

	@Override
	public Buffer next() {
		byte[] buffer = new byte[bufferSize];
		int bytesRead = read(buffer);
		checkState(bytesRead >= 0, "unexpected end of file, file = " + file.getFile() + ", offset=" + offset);
		offset += bytesRead;
		bytesToRead -= bytesRead;
		return new NetworkBuffer(wrap(buffer), FreeingBufferRecycler.INSTANCE, DATA_BUFFER, bytesRead);
	}

	private int read(byte[] buffer) {
		int limit = Math.min(buffer.length, bytesToRead);
		try {
			return stream.read(buffer, 0, limit);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws Exception {
		closeAll(stream, file::release);
	}
}
