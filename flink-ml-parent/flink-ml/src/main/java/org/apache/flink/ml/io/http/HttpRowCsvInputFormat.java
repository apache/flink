/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.io.http;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;

/**
 * Http Row Csv InputFormat.
 */
public class HttpRowCsvInputFormat implements InputFormat <Row, HttpFileInputSplit> {

	private String path;
	private String charsetName = "UTF-8";
	private String fieldDelimStr;
	private TypeInformation <?>[] fieldTypes;
	private final boolean ignoreFirstLine;

	// --------------------------------------------------------------------------------------------
	//  Variables for internal parsing.
	//  They are all transient, because we do not want them so be serialized
	// --------------------------------------------------------------------------------------------

	// for reading records
	private transient Charset charset;
	private transient InputStream stream;
	private transient int splitLength;
	private transient byte[] readBuffer;
	private transient byte[] wrapBuffer;
	private transient int readPos;
	private transient int limit;
	private transient byte[] currBuffer;        // buffer in which current record byte sequence is found
	private transient int currOffset;            // offset in above buffer
	private transient int currLen;                // length of current byte sequence
	private transient boolean overLimit;
	private transient boolean end;
	private long offset = -1;

	private transient HttpURLConnection connection;
	private transient HttpFileInputSplit split;
	private transient long bytesRead;  // number of bytes read from http connection

	// for parsing fields of a reacord
	private transient FieldParser <?>[] fieldParsers = null;
	private transient Object[] holders = null;
	private byte[] fieldDelim;

	// The delimiter may be set with a byte-sequence or a String. In the latter
	// case the byte representation is updated consistent with current charset.
	private byte[] delimiter = new byte[] {'\n'};
	private int lineLengthLimit = 1024 * 1024;
	private int bufferSize = 1024 * 1024;

	public HttpRowCsvInputFormat(
		String path, TypeInformation <?>[] fieldTypes, String fieldDelim, String rowDelim, boolean ignoreFirstLine) {
		this.path = path;
		this.fieldTypes = fieldTypes;
		this.charset = Charset.forName(charsetName);
		this.fieldDelim = fieldDelim.getBytes(charset);
		this.fieldDelimStr = fieldDelim;
		assert (rowDelim.equals("\n"));
		this.ignoreFirstLine = ignoreFirstLine;
	}

	private void openConnection(HttpFileInputSplit split, long start) {
		try {
			String path = split.path;
			URL url = new URL(path);

			this.connection = (HttpURLConnection) url.openConnection();
			this.connection.setDoInput(true);
			this.connection.setConnectTimeout(5000);
			this.connection.setReadTimeout(60000);
			this.connection.setRequestMethod("GET");
			if (split.splitable) {
				if (start < 0) {
					start = split.start;
				}
				this.connection.setRequestProperty("If-Range", split.etag);
				this.connection.setRequestProperty("Range", String.format("bytes=%d-%d", start, split.end - 1));
			}
			this.connection.connect();
			this.stream = this.connection.getInputStream();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void closeConnection() throws IOException {
		if (this.stream != null) {
			this.stream.close();
		}
		if (this.connection != null) {
			this.connection.disconnect();
		}
	}

	private void initBuffers() {
		if (this.bufferSize <= this.delimiter.length) {
			throw new IllegalArgumentException("Buffer size must be greater than length of delimiter.");
		}

		if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
			this.readBuffer = new byte[this.bufferSize];
		}
		if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
			this.wrapBuffer = new byte[256];
		}

		this.readPos = 0;
		this.limit = 0;
		this.overLimit = false;
		this.end = false;
	}

	private void initializeParsers() {
		Class <?>[] fieldClasses = extractTypeClasses(fieldTypes);

		// instantiate the parsers
		FieldParser <?>[] parsers = new FieldParser <?>[fieldClasses.length];

		for (int i = 0; i < fieldClasses.length; i++) {
			if (fieldClasses[i] != null) {
				Class <? extends FieldParser <?>> parserType = FieldParser.getParserForType(fieldClasses[i]);
				if (parserType == null) {
					throw new RuntimeException("No parser available for type '" + fieldClasses[i].getName() + "'.");
				}

				FieldParser <?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);
				p.setCharset(charset);
				parsers[i] = p;
			}
		}
		this.fieldParsers = parsers;
		this.holders = new Object[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			holders[i] = fieldParsers[i].createValue();
		}
	}

	/**
	 * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	 * and positions the stream at the correct position, making sure that any partial record at the beginning is
	 * skipped.
	 *
	 * @param split The input split to open.
	 * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	 */
	@Override
	public void open(HttpFileInputSplit split) throws IOException {
		this.charset = Charset.forName(charsetName);
		this.splitLength = (int) split.length;
		this.split = split;
		this.bytesRead = 0L;

		initBuffers();
		openConnection(split, -1);
		initializeParsers();

		this.offset = 0;
		if (split.splitNo > 0 || ignoreFirstLine) {
			readLine();
			// if the first partial record already pushes the stream over
			// the limit of our split, then no record starts within this split
			if (this.overLimit) {
				this.end = true;
			}
		} else {
			fillBuffer(0);
		}
	}

	/**
	 * Closes the input by releasing all buffers and closing the file input stream.
	 *
	 * @throws IOException Thrown, if the closing of the file stream causes an I/O error.
	 */
	@Override
	public void close() throws IOException {
		this.wrapBuffer = null;
		this.readBuffer = null;
		closeConnection();
	}

	/**
	 * Checks whether the current split is at its end.
	 *
	 * @return True, if the split is at its end, false otherwise.
	 */
	@Override
	public boolean reachedEnd() {
		return this.end;
	}

	//    private int debug_cnt = 0;

	@Override
	public Row nextRecord(Row record) throws IOException {
		if (readLine()) {
			if (0 == this.currLen && fieldTypes.length > 1) {
				// current line is empty, then skip
				return nextRecord(record);
			} else {
				return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
			}
		} else {
			this.end = true;
			return null;
		}
	}

	protected final boolean readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return false;
		}

		int countInWrapBuffer = 0;

		// position of matching positions in the delimiter byte array
		int delimPos = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				// readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
				if (!fillBuffer(delimPos)) {
					int countInReadBuffer = delimPos;
					if (countInWrapBuffer + countInReadBuffer > 0) {
						// we have bytes left to emit
						if (countInReadBuffer > 0) {
							// we have bytes left in the readBuffer. Move them into the wrapBuffer
							if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
								// reallocate
								byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
								System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
								this.wrapBuffer = tmp;
							}

							// copy readBuffer bytes to wrapBuffer
							System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer,
								countInReadBuffer);
							countInWrapBuffer += countInReadBuffer;
						}

						this.offset += countInWrapBuffer;
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos - delimPos;
			int count;

			// Search for next occurence of delimiter in read buffer.
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else {
					// Delimiter does not match.
					// We have to reset the read position to the character after the first matching character
					//   and search for the whole delimiter again.
					readPos -= delimPos;
					delimPos = 0;
				}
				readPos++;
			}

			// check why we dropped out
			if (delimPos == this.delimiter.length) {
				// we found a delimiter
				int readBufferBytesRead = this.readPos - startPos;
				this.offset += countInWrapBuffer + readBufferBytesRead;
				count = readBufferBytesRead - this.delimiter.length;

				// copy to byte array
				if (countInWrapBuffer > 0) {
					// check wrap buffer size
					if (this.wrapBuffer.length < countInWrapBuffer + count) {
						final byte[] nb = new byte[countInWrapBuffer + count];
						System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
						this.wrapBuffer = nb;
					}
					if (count >= 0) {
						System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
					}
					setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
					return true;
				} else {
					setResult(this.readBuffer, startPos, count);
					return true;
				}
			} else {
				// we reached the end of the readBuffer
				count = this.limit - startPos;

				// check against the maximum record length
				if (((long) countInWrapBuffer) + count > this.lineLengthLimit) {
					throw new IOException("The record length exceeded the maximum record length (" +
						this.lineLengthLimit + ").");
				}

				// Compute number of bytes to move to wrapBuffer
				// Chars of partially read delimiter must remain in the readBuffer. We might need to go back.
				int bytesToMove = count - delimPos;
				// ensure wrapBuffer is large enough
				if (this.wrapBuffer.length - countInWrapBuffer < bytesToMove) {
					// reallocate
					byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2, countInWrapBuffer + bytesToMove)];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;
				}

				// copy readBuffer to wrapBuffer (except delimiter chars)
				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
				countInWrapBuffer += bytesToMove;
				// move delimiter chars to the beginning of the readBuffer
				System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);

			}
		}
	}

	private void setResult(byte[] buffer, int offset, int len) {
		this.currBuffer = buffer;
		this.currOffset = offset;
		this.currLen = len;
	}

	/**
	 * Fills the read buffer with bytes read from the file starting from an offset.
	 */
	private boolean fillBuffer(int offset) throws IOException {
		int maxReadLength = this.readBuffer.length - offset;
		int toRead;
		if (this.splitLength > 0) {
			// if we have more data, read that
			toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
		} else {
			// if we have exhausted our split, we need to complete the current record, or read one
			// more across the next split.
			// the reason is that the next split will skip over the beginning until it finds the first
			// delimiter, discarding it as an incomplete chunk of data that belongs to the last record in the
			// previous split.
			toRead = maxReadLength;
			this.overLimit = true;
		}

		int tryTimes = 0;
		int read = -1;
		while (this.bytesRead + this.split.start < this.split.end && read == -1 && tryTimes < 10) {
			read = this.stream.read(this.readBuffer, offset, toRead);

			// unexpected EOF encountered, re-establish the connection
			if (read == -1) {
				this.closeConnection();
				this.openConnection(this.split, this.split.start + bytesRead);
			}

			tryTimes++;
		}

		if (tryTimes >= 10) {
			throw new RuntimeException("Fail to read from http connection after trying 10 times.");
		}

		if (tryTimes > 1) {
			System.out.println(
				String.format("fillBuffer: toRead = %d, read = %d, tryTimes = %d, remainig split length %d",
					toRead, read, tryTimes, splitLength));
		}

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.splitLength -= read;
			this.readPos = offset; // position from where to start reading
			this.limit = read + offset; // number of valid bytes in the read buffer
			this.bytesRead += read;
			return true;
		}
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public HttpFileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		/* called by JobManager */

		URL url = new URL(path);
		HttpURLConnection headerConnection = (HttpURLConnection) url.openConnection();
		headerConnection.setConnectTimeout(5000);
		headerConnection.setRequestMethod("HEAD");
		HttpFileInputSplit[] splits;

		headerConnection.connect();
		long contentLength = headerConnection.getContentLengthLong();
		String acceptRanges = headerConnection.getHeaderField("Accept-Ranges");
		String contentRange = headerConnection.getHeaderField("Content-Range");
		String etag = headerConnection.getHeaderField("ETag");
		String contentType = headerConnection.getHeaderField("Content-Type");

		if (contentLength < 0) {
			throw new RuntimeException("Fail to get content length: " + path);
		}

		boolean splitable = (acceptRanges != null && acceptRanges.equalsIgnoreCase("bytes")
			&& etag != null && contentLength >= 0);

		// If the http server does not accept ranges, then we quit the program.
		// This is because 'accept ranges' is required to achieve robustness (through re-connection),
		// and efficiency (through concurrent read).
		if (!splitable) {
			throw new RuntimeException("The http server does not support range reading.");
		}

		splits = new HttpFileInputSplit[minNumSplits];
		for (int i = 0; i < splits.length; i++) {
			splits[i] = new HttpFileInputSplit(path, minNumSplits, i, true,
				contentLength, etag);
		}

		headerConnection.disconnect();
		return splits;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HttpFileInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	private static Class <?>[] extractTypeClasses(TypeInformation[] fieldTypes) {
		Class <?>[] classes = new Class <?>[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			classes[i] = fieldTypes[i].getTypeClass();
		}
		return classes;
	}

	protected Row readRecord(Row reuse, byte[] bytes, int offset, int numBytes) throws IOException {
		Row reuseRow;
		if (reuse == null) {
			reuseRow = new Row(fieldTypes.length);
		} else {
			reuseRow = reuse;
		}

		// Found window's end line, so find carriage return before the newline
		if (numBytes > 0 && bytes[offset + numBytes - 1] == '\r') {
			//reduce the number of bytes so that the Carriage return is not taken as data
			numBytes--;
		}

		int startPos = offset;
		for (int field = 0; field < fieldTypes.length; field++) {
			FieldParser <Object> parser = (FieldParser <Object>) fieldParsers[field];

			int newStartPos = parser.resetErrorStateAndParse(
				bytes,
				startPos,
				offset + numBytes,
				fieldDelim,
				holders[field]);

			if (parser.getErrorState() != FieldParser.ParseErrorState.NONE) {
				if (parser.getErrorState() == FieldParser.ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR) {
					reuseRow.setField(field, null);
				} else if (parser.getErrorState() == FieldParser.ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER) {
					reuseRow.setField(field, null);
				} else if (parser.getErrorState() == FieldParser.ParseErrorState.EMPTY_COLUMN) {
					reuseRow.setField(field, null);
				} else {
					throw new ParseException(
						String.format("Parsing error for column %1$s of row '%2$s' originated by %3$s: %4$s.",
							field, new String(bytes, offset, numBytes), parser.getClass().getSimpleName(),
							parser.getErrorState()));
				}
			} else {
				reuseRow.setField(field, parser.getLastResult());
			}

			if (newStartPos < 0) {
				if (field < fieldTypes.length - 1) { // skip next field delimiter
					while (startPos + fieldDelim.length <= offset + numBytes && (!FieldParser.delimiterNext(bytes,
						startPos, fieldDelim))) {
						startPos++;
					}
					if (startPos + fieldDelim.length > offset + numBytes) {
						throw new RuntimeException("Can't find next field delimiter: " + "\"" + fieldDelimStr + "\","
							+ " " +
							"Perhaps the data is invalid or do not match the schema." +
							"The row is: " + new String(bytes, offset, numBytes));
					}
					startPos += fieldDelim.length;
				}
			} else {
				startPos = newStartPos;
			}
		}

		return reuseRow;
	}

}
