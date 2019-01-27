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

package org.apache.flink.runtime.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentWritable;
import org.apache.flink.util.FileUtils;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

/**
 * Buffered output.
 */
public class NioBufferedFileOutputStream implements MemorySegmentWritable {

	private final FileChannel fileChannel;
	private final ByteBuffer writeBuffer;

	private volatile boolean closed;

	public NioBufferedFileOutputStream(FileChannel fileChannel, int bufferSize) {
		this.fileChannel = fileChannel;
		this.writeBuffer = ByteBuffer.allocateDirect(bufferSize);
	}

	@Override
	public void write(MemorySegment segment, int off, int len) throws IOException {
		write(segment.wrap(off, len));
	}

	public void write(ByteBuffer buffer) throws IOException {
		while (buffer.remaining() > 0) {
			int toCopy = Math.min(buffer.remaining(), writeBuffer.remaining());
			if (toCopy > 0) {
				ByteBuffer src = buffer.duplicate();
				src.limit(src.position() + toCopy);
				writeBuffer.put(src);
				buffer.position(src.position());
			}
			if (writeBuffer.remaining() == 0) {
				flush();
			}
		}
	}

	public void flush() throws IOException {
		if (!closed) {
			// now flush
			if (writeBuffer.position() > 0) {
				writeBuffer.flip();
				FileUtils.writeCompletely(fileChannel, writeBuffer);
				writeBuffer.clear();
			}
		} else {
			throw new IOException("closed");
		}
	}

	public void close() throws IOException {
		if (!closed) {
			flush();
			closed = true;
			if (writeBuffer != null && writeBuffer instanceof MappedByteBuffer) {
				Cleaner cleaner = ((DirectBuffer) writeBuffer).cleaner();
				if (cleaner != null) {
					cleaner.clean();
				}
			}
		}
	}
}
