/*
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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.EOFException;
import java.io.IOException;

/**
 * Abstract base class for wrappers over multiple {@link FSDataInputStream}, which gives a contiguous view on all inner
 * streams and makes them look like a single stream, in which we can read, seek, etc.
 */
@Internal
public abstract class AbstractMultiFSDataInputStream extends FSDataInputStream {

	/** Inner stream for the currently accessed segment of the virtual global stream. */
	protected FSDataInputStream delegate;

	/** Position in the virtual global stream. */
	protected long totalPos;

	/** Total available bytes in the virtual global stream. */
	protected long totalAvailable;

	public AbstractMultiFSDataInputStream() {
		this.totalPos = 0L;
	}

	@Override
	public void seek(long desired) throws IOException {

		if (desired == totalPos) {
			return;
		}

		Preconditions.checkArgument(desired >= 0L);

		if (desired > totalAvailable) {
			throw new EOFException();
		}

		IOUtils.closeQuietly(delegate);
		delegate = getSeekedStreamForOffset(desired);

		this.totalPos = desired;
	}

	@Override
	public long getPos() throws IOException {
		return totalPos;
	}

	@Override
	public int read() throws IOException {

		if (null == delegate) {
			return -1;
		}

		int val = delegate.read();

		if (-1 == val) {
			IOUtils.closeQuietly(delegate);
			if (totalPos < totalAvailable) {
				delegate = getSeekedStreamForOffset(totalPos);
			} else {
				delegate = null;
			}
			return read();
		}

		++totalPos;
		return val;
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(delegate);
	}

	@Override
	public long skip(long n) throws IOException {
		seek(totalPos + n);
		return n;
	}

	/**
	 * Delivers a the right stream for the given global stream offset. The returned stream is already seeked to the
	 * right local offset that correctly reflects the global offset.
	 *
	 * @param globalStreamOffset the global offset to which we seek
	 * @return a sub-stream, seeked to the correct local offset w.r.t. the global offset.
	 * @throws IOException
	 */
	protected abstract FSDataInputStream getSeekedStreamForOffset(long globalStreamOffset) throws IOException;
}
