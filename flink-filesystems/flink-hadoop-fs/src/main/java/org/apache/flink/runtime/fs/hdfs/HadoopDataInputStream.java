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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Concrete implementation of the {@link FSDataInputStream} for Hadoop's input streams.
 * This supports all file systems supported by Hadoop, such as HDFS and S3 (S3a/S3n).
 */
public final class HadoopDataInputStream extends FSDataInputStream {

	/**
	 * Minimum amount of bytes to skip forward before we issue a seek instead of discarding read.
	 *
	 * <p>The current value is just a magic number. In the long run, this value could become configurable, but for now it
	 * is a conservative, relatively small value that should bring safe improvements for small skips (e.g. in reading
	 * meta data), that would hurt the most with frequent seeks.
	 *
	 * <p>The optimal value depends on the DFS implementation and configuration plus the underlying filesystem.
	 * For now, this number is chosen "big enough" to provide improvements for smaller seeks, and "small enough" to
	 * avoid disadvantages over real seeks. While the minimum should be the page size, a true optimum per system would
	 * be the amounts of bytes the can be consumed sequentially within the seektime. Unfortunately, seektime is not
	 * constant and devices, OS, and DFS potentially also use read buffers and read-ahead.
	 */
	public static final int MIN_SKIP_BYTES = 1024 * 1024;

	/** The internal stream. */
	private final org.apache.hadoop.fs.FSDataInputStream fsDataInputStream;

	/**
	 * Creates a new data input stream from the given Hadoop input stream.
	 *
	 * @param fsDataInputStream The Hadoop input stream
	 */
	public HadoopDataInputStream(org.apache.hadoop.fs.FSDataInputStream fsDataInputStream) {
		this.fsDataInputStream = checkNotNull(fsDataInputStream);
	}

	@Override
	public void seek(long seekPos) throws IOException {
		// We do some optimizations to avoid that some implementations of distributed FS perform
		// expensive seeks when they are actually not needed.
		long delta = seekPos - getPos();

		if (delta > 0L && delta <= MIN_SKIP_BYTES) {
			// Instead of a small forward seek, we skip over the gap
			skipFully(delta);
		} else if (delta != 0L) {
			// For larger gaps and backward seeks, we do a real seek
			forceSeek(seekPos);
		} // Do nothing if delta is zero.
	}

	@Override
	public long getPos() throws IOException {
		return fsDataInputStream.getPos();
	}

	@Override
	public int read() throws IOException {
		return fsDataInputStream.read();
	}

	@Override
	public void close() throws IOException {
		fsDataInputStream.close();
	}

	@Override
	public int read(@Nonnull byte[] buffer, int offset, int length) throws IOException {
		return fsDataInputStream.read(buffer, offset, length);
	}

	@Override
	public int available() throws IOException {
		return fsDataInputStream.available();
	}

	@Override
	public long skip(long n) throws IOException {
		return fsDataInputStream.skip(n);
	}

	/**
	 * Gets the wrapped Hadoop input stream.
	 * @return The wrapped Hadoop input stream.
	 */
	public org.apache.hadoop.fs.FSDataInputStream getHadoopInputStream() {
		return fsDataInputStream;
	}

	/**
	 * Positions the stream to the given location. In contrast to {@link #seek(long)}, this method will
	 * always issue a "seek" command to the dfs and may not replace it by {@link #skip(long)} for small seeks.
	 *
	 * <p>Notice that the underlying DFS implementation can still decide to do skip instead of seek.
	 *
	 * @param seekPos the position to seek to.
	 * @throws IOException
	 */
	public void forceSeek(long seekPos) throws IOException {
		fsDataInputStream.seek(seekPos);
	}

	/**
	 * Skips over a given amount of bytes in the stream.
	 *
	 * @param bytes the number of bytes to skip.
	 * @throws IOException
	 */
	public void skipFully(long bytes) throws IOException {
		while (bytes > 0) {
			bytes -= fsDataInputStream.skip(bytes);
		}
	}
}
