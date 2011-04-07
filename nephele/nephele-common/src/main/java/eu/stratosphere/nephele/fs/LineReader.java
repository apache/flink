/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.nephele.fs;

import java.io.IOException;

import eu.stratosphere.nephele.fs.FSDataInputStream;

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

	public LineReader(FSDataInputStream strm, long start, long length, int buffersize)
																						throws IOException {
		this.stream = strm;
		readBuffer = new byte[buffersize];
		wrapBuffer = new byte[256];

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
		int toRead = lengthLeft > readBuffer.length ? readBuffer.length : (int) lengthLeft;
		if (lengthLeft <= 0) {
			toRead = readBuffer.length;
			overLimit = true;
		}

		int read = stream.read(readBuffer, 0, toRead);

		if (read == -1) {
			stream.close();
			stream = null;
			return false;
		} else {
			lengthLeft -= read;
			readPos = 0;
			limit = read;
			return true;
		}

	}

	public void close() throws IOException {
		wrapBuffer = null;
		readBuffer = null;
		if (stream != null)
			stream.close();
	}

	public byte[] readLine() throws IOException {
		if (stream == null || overLimit) {
			return null;
		}

		int curr = 0;
		int countInWrapBuffer = 0;

		while (true) {
			if (readPos >= limit) {
				if (!fillBuffer()) {
					if (countInWrapBuffer > 0) {
						byte[] tmp = new byte[countInWrapBuffer];
						System.arraycopy(wrapBuffer, 0, tmp, 0, countInWrapBuffer);
						return tmp;
					} else {
						return null;
					}
				}
			}

			int startPos = readPos;
			int count = 0;

			while (readPos < limit && (curr = readBuffer[readPos++]) != LF)
				;

			// check why we dropped out
			if (curr == LF) {
				// line end
				count = readPos - startPos - 1;
				if (readPos == 1 && countInWrapBuffer > 0 && wrapBuffer[countInWrapBuffer - 1] == CR) {
					countInWrapBuffer--;
				} else if (readPos > startPos + 1 && readBuffer[readPos - 2] == CR) {
					count--;
				}

				// copy to byte array
				if (countInWrapBuffer > 0) {
					byte[] end = new byte[countInWrapBuffer + count];
					System.arraycopy(wrapBuffer, 0, end, 0, countInWrapBuffer);
					System.arraycopy(readBuffer, 0, end, countInWrapBuffer, count);
					return end;
				} else {
					byte[] end = new byte[count];
					System.arraycopy(readBuffer, startPos, end, 0, count);
					return end;
				}
			} else {
				count = limit - startPos;

				// buffer exhausted
				while (wrapBuffer.length - countInWrapBuffer < count) {
					// reallocate
					byte[] tmp = new byte[wrapBuffer.length * 2];
					System.arraycopy(wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					wrapBuffer = tmp;
				}

				System.arraycopy(readBuffer, startPos, wrapBuffer, countInWrapBuffer, count);
				countInWrapBuffer += count;
			}
		}
	}
}