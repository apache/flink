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

package org.apache.flink.table.runtime.util;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;

import java.io.IOException;

/**
 * ChannelWriterOutputView to abstract compression and no compression.
 */
public abstract class AbstractChannelWriterOutputView extends AbstractPagedOutputView {

	private final FileIOChannel writer;

	AbstractChannelWriterOutputView(FileIOChannel writer, int segmentSize, int headerLength) {
		super(segmentSize, headerLength);
		this.writer = writer;
	}

	public abstract int close() throws IOException;

	/**
	 * Closes the underlying channel and deletes the underlying file.
	 */
	public void closeAndDelete() throws IOException {
		writer.closeAndDelete();
	}

	/**
	 * Gets the number of blocks used by this view.
	 */
	public abstract int getBlockCount();

	/**
	 * Get output bytes.
	 */
	public abstract long getNumBytes() throws IOException;

	/**
	 * Get output compressed bytes, return num bytes if there is no compression.
	 */
	public abstract long getNumCompressedBytes() throws IOException;

	public void deleteChannel() {
		writer.deleteChannel();
	}

	public FileIOChannel.ID getChannelID() {
		return writer.getChannelID();
	}
}
