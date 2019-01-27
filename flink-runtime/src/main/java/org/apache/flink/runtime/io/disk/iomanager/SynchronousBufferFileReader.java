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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * A synchronous {@link BufferFileReader} implementation.
 *
 * <p> This currently bypasses the I/O manager as it is the only synchronous implementation, which
 * is currently in use.
 *
 * TODO Refactor I/O manager setup and refactor this into it
 */
public class SynchronousBufferFileReader extends SynchronousFileIOChannel implements BufferFileReader {

	private final BufferFileChannelReader reader;

	private boolean hasReachedEndOfFile;

	private final boolean withHeader;
	private long currentPosition; // Used only when withHeader == false.

	public SynchronousBufferFileReader(ID channelID, boolean writeEnabled) throws IOException {
		this(channelID, writeEnabled, true);
	}

	public SynchronousBufferFileReader(ID channelID, boolean writeEnabled, boolean withHeader) throws IOException {
		super(channelID, writeEnabled);
		this.reader = new BufferFileChannelReader(fileChannel);
		this.withHeader = withHeader;
	}

	@Override
	public void readInto(Buffer buffer) throws IOException {
		if (withHeader) {
			if (fileChannel.size() - fileChannel.position() > 0) {
				hasReachedEndOfFile = reader.readBufferFromFileChannel(buffer);
			}
			else {
				buffer.recycleBuffer();
			}
		} else {
			throw new IllegalArgumentException("Method readInto(Buffer buffer) only support withHeader mode.");
		}
	}

	@Override
	public void readInto(Buffer buffer, long length) throws IOException {
		if (!withHeader) {
			if (fileChannel.size() - currentPosition > 0) {
				hasReachedEndOfFile = StreamFileChannelReader.readBufferFromFileChannel(
					fileChannel, buffer, currentPosition, length, id.getPathFile());
				currentPosition += length;
			} else {
				buffer.recycleBuffer();
				// In case of empty file.
				if (!hasReachedEndOfFile) {
					hasReachedEndOfFile = true;
				}
			}
		} else {
			throw new IllegalArgumentException("Method readInto(Buffer buffer, long length) only support withoutHeader mode.");
		}
	}

	@Override
	public void seekToPosition(long position) throws IOException {
		currentPosition = position;
		fileChannel.position(position);
	}

	@Override
	public boolean hasReachedEndOfFile() {
		return hasReachedEndOfFile;
	}
}
