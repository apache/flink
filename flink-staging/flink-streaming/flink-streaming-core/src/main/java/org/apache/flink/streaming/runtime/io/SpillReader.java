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

package org.apache.flink.streaming.runtime.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

public class SpillReader {

	private FileChannel spillingChannel;
	private File spillFile;

	/**
	 * Reads the next buffer from the spilled file.
	 */
	public Buffer readNextBuffer(int bufferSize) throws IOException {
		try {
			Buffer buffer = new Buffer(new MemorySegment(new byte[bufferSize]),
					new BufferRecycler() {

						@Override
						public void recycle(MemorySegment memorySegment) {
							memorySegment.free();
						}
					});

			spillingChannel.read(buffer.getMemorySegment().wrap(0, bufferSize));

			return buffer;
		} catch (Exception e) {
			close();
			throw new IOException(e);
		}
	}

	@SuppressWarnings("resource")
	public void setSpillFile(File nextSpillFile) throws IOException {
		// We can close and delete the file now
		close();
		if (spillFile != null) {
			spillFile.delete();
		}
		this.spillFile = nextSpillFile;
		this.spillingChannel = new RandomAccessFile(spillFile, "rw").getChannel();
	}

	public File getSpillFile() {
		return spillFile;
	}

	public void close() throws IOException {
		if (this.spillingChannel != null && this.spillingChannel.isOpen()) {
			this.spillingChannel.close();
		}
	}

}
