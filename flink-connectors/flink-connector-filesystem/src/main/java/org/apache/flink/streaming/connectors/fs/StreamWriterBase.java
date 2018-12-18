/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Base class for {@link Writer Writers} that write to a {@link FSDataOutputStream}.
 */
public abstract class StreamWriterBase<T> implements Writer<T> {

	private static final long serialVersionUID = 2L;

	/**
	 * The {@code FSDataOutputStream} for the current part file.
	 */
	private transient FSDataOutputStream outStream;

	private boolean syncOnFlush;

	public StreamWriterBase() {
	}

	protected StreamWriterBase(StreamWriterBase<T> other) {
		this.syncOnFlush = other.syncOnFlush;
	}

	/**
	 * Controls whether to sync {@link FSDataOutputStream} on flush.
	 */
	public void setSyncOnFlush(boolean syncOnFlush) {
		this.syncOnFlush = syncOnFlush;
	}

	/**
	 * Returns the current output stream, if the stream is open.
	 */
	protected FSDataOutputStream getStream() {
		if (outStream == null) {
			throw new IllegalStateException("Output stream has not been opened");
		}
		return outStream;
	}

	@Override
	public void open(FileSystem fs, Path path) throws IOException {
		if (outStream != null) {
			throw new IllegalStateException("Writer has already been opened");
		}
		outStream = fs.create(path, false);
	}

	@Override
	public long flush() throws IOException {
		if (outStream == null) {
			throw new IllegalStateException("Writer is not open");
		}
		if (syncOnFlush) {
			outStream.hsync();
		}
		else {
			outStream.hflush();
		}
		return outStream.getPos();
	}

	@Override
	public long getPos() throws IOException {
		if (outStream == null) {
			throw new IllegalStateException("Writer is not open");
		}
		return outStream.getPos();
	}

	@Override
	public void close() throws IOException {
		if (outStream != null) {
			flush();
			outStream.close();
			outStream = null;
		}
	}

	public boolean isSyncOnFlush() {
		return syncOnFlush;
	}
}
