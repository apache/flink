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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.compress.utils.BoundedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input format that reads csv. This abstract class is responsible for cutting the boundary of
 * InputSplit.
 */
public abstract class AbstractCsvInputFormat<T> extends FileInputFormat<T> {

	protected final CsvSchema csvSchema;
	protected transient InputStream csvInputStream;

	public AbstractCsvInputFormat(Path[] filePaths, CsvSchema csvSchema) {
		setFilePaths(filePaths);
		this.csvSchema = checkNotNull(csvSchema);
	}

	@Override
	public boolean supportsMultiPaths() {
		return true;
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		csvInputStream = stream;

		long csvStart = splitStart;
		if (splitStart != 0) {
			csvStart = findNextLineStartOffset();
		}

		if (splitLength != READ_WHOLE_SPLIT_FLAG) {
			stream.seek(splitStart + splitLength);
			long nextLineStartOffset = findNextLineStartOffset();
			stream.seek(csvStart);
			csvInputStream = new BoundedInputStream(stream, nextLineStartOffset - csvStart);
		} else {
			stream.seek(csvStart);
		}
	}

	/**
	 * Find next legal line separator to return next offset (first byte offset of next line).
	 *
	 * <p>NOTE: Because of the particularity of UTF-8 encoding, we can determine the number of bytes
	 * of this character only by comparing the first byte, so we do not need to traverse M*N in comparison.
	 */
	private long findNextLineStartOffset() throws IOException {
		boolean usesEscapeChar = csvSchema.usesEscapeChar();
		byte[] escapeBytes = Character.toString((char) csvSchema.getEscapeChar())
				.getBytes(StandardCharsets.UTF_8);
		long startPos = stream.getPos();

		byte b;
		while ((b = (byte) stream.read()) != -1) {
			if (b == '\r' || b == '\n') {
				// If there may be escape tags ahead
				if (usesEscapeChar && stream.getPos() - startPos <= escapeBytes.length) {
					long front = stream.getPos() - escapeBytes.length - 1;
					if (front > 0) {
						stream.seek(front);
						byte[] readBytes = new byte[escapeBytes.length];
						stream.read(readBytes); // we have judge front must bigger than zero
						stream.read(); // back to current next one
						if (Arrays.equals(escapeBytes, readBytes)) {
							// equal, we should skip this one line separator
							continue;
						}
					}
				}

				long pos = stream.getPos();

				// deal with "\r\n", next one maybe '\n', so we need skip it.
				if (b == '\r' && (byte) stream.read() == '\n') {
					return stream.getPos();
				} else {
					return pos;
				}
			} else if (usesEscapeChar && b == escapeBytes[0]) {
				boolean equal = true;
				for (int i = 1; i < escapeBytes.length; i++) {
					if ((byte) stream.read() != escapeBytes[i]) {
						equal = false;
						break;
					}
				}
				if (equal) {
					// equal, we should skip next one
					stream.skip(1);
				}
			}
		}
		return stream.getPos();
	}
}
