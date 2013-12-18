/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.runtime.fs;

import java.io.IOException;

import eu.stratosphere.core.fs.FSDataInputStream;

public class LineReader {

	private static final int CR = '\r';

	private static final int LF = '\n';

	private FSDataInputStream stream;

	private byte[] readBuffer;

	private byte[] wrapBuffer;

	private long lengthLeft;

	private int readPos;

	private int limit;

	private boolean overLimit;

	public LineReader(final FSDataInputStream strm, final long start, final long length, final int buffersize)
			throws IOException {
		this.stream = strm;
		this.readBuffer = new byte[buffersize];
		this.wrapBuffer = new byte[256];

		this.lengthLeft = length;
		this.readPos = 0;
		this.overLimit = false;

		if (start != 0) {
			strm.seek(start);
			readLine();
		} else {
			fillBuffer();
		}
	}

	private final boolean fillBuffer() throws IOException {

		int toRead = this.lengthLeft > this.readBuffer.length ? this.readBuffer.length : (int) this.lengthLeft;
		if (this.lengthLeft <= 0) {
			toRead = this.readBuffer.length;
			this.overLimit = true;
		}

		int read = this.stream.read(this.readBuffer, 0, toRead);

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.lengthLeft -= read;
			this.readPos = 0;
			this.limit = read;
			return true;
		}

	}

	public void close() throws IOException {
		this.wrapBuffer = null;
		this.readBuffer = null;
		if (this.stream != null) {
			this.stream.close();
		}
	}

	public byte[] readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return null;
		}

		int curr = 0;
		int countInWrapBuffer = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				if (!fillBuffer()) {
					if (countInWrapBuffer > 0) {
						byte[] tmp = new byte[countInWrapBuffer];
						System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
						return tmp;
					} else {
						return null;
					}
				}
			}

			int startPos = this.readPos;
			int count = 0;

			while (this.readPos < this.limit && (curr = this.readBuffer[this.readPos++]) != LF) {
			}

			// check why we dropped out
			if (curr == LF) {
				// line end
				count = this.readPos - startPos - 1;
				if (this.readPos == 1 && countInWrapBuffer > 0 && this.wrapBuffer[countInWrapBuffer - 1] == CR) {
					countInWrapBuffer--;
				} else if (this.readPos > startPos + 1 && this.readBuffer[this.readPos - 2] == CR) {
					count--;
				}

				// copy to byte array
				if (countInWrapBuffer > 0) {
					byte[] end = new byte[countInWrapBuffer + count];
					System.arraycopy(this.wrapBuffer, 0, end, 0, countInWrapBuffer);
					System.arraycopy(this.readBuffer, 0, end, countInWrapBuffer, count);
					return end;
				} else {
					byte[] end = new byte[count];
					System.arraycopy(this.readBuffer, startPos, end, 0, count);
					return end;
				}
			} else {
				count = this.limit - startPos;

				// buffer exhausted
				while (this.wrapBuffer.length - countInWrapBuffer < count) {
					// reallocate
					byte[] tmp = new byte[this.wrapBuffer.length * 2];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;
				}

				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, count);
				countInWrapBuffer += count;
			}
		}
	}
}
