/**
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


package org.apache.flink.api.java.record.io;


import java.io.IOException;

import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Record;

/**
 * 
 */
public abstract class FixedLengthInputFormat extends FileInputFormat {
	private static final long serialVersionUID = 1L;
	
	/**
	 * The config parameter which defines the fixed length of a record.
	 */
	public static final String RECORDLENGTH_PARAMETER_KEY = "pact.fix-input.record-length";
	
	/**
	 * The default read buffer size = 1MB.
	 */
	private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;
	
	/**
	 * Buffer to read a batch of records from a file 
	 */
	private byte[] readBuffer;
	
	/**
	 * The position in the stream
	 */
	private long streamPos;
	
	/**
	 * The end position in the stream.
	 */
	private long streamEnd;
	
	/**
	 * read position within the read buffer
	 */
	private int readBufferPos;
	
	/**
	 * The limit of the data in the read buffer.
	 */
	private int readBufferLimit;
	
	/**
	 * fixed length of all records
	 */
	private int recordLength;
	
	/**
	 * size of the read buffer
	 */
	private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;
	
	/**
	 * The flag whether the stream is exhausted.
	 */
	private boolean exhausted;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructor only sets the key and value classes
	 */
	protected FixedLengthInputFormat() {}
	
	/**
	 * Reads a record out of the given buffer. This operation always consumes the standard number of
	 * bytes, regardless of whether the produced record was valid.
	 * 
	 * @param target The target Record
	 * @param buffer The buffer containing the binary data.
	 * @param startPos The start position in the byte array.
	 * @return True, is the record is valid, false otherwise.
	 */
	public abstract boolean readBytes(Record target, byte[] buffer, int startPos);
	
	/**
	 * Returns the fixed length of a record.
	 * 
	 * @return the fixed length of a record.
	 */
	public int getRecordLength() {
		return this.recordLength;
	}
	
	/**
	 * Gets the size of the buffer internally used to parse record boundaries.
	 * 
	 * @return The size of the parsing buffer.
	 */
	public int getReadBufferSize() {
		return this.readBuffer.length;
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public void configure(Configuration parameters) {
		// pass parameters to FileInputFormat
		super.configure(parameters);

		// read own parameters
		this.recordLength = parameters.getInteger(RECORDLENGTH_PARAMETER_KEY, 0);
		if (recordLength < 1) {
			throw new IllegalArgumentException("The record length parameter must be set and larger than 0.");
		}
	}
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		// open input split using FileInputFormat
		super.open(split);
		
		// adjust the stream positions for boundary splits
		int recordOffset = (int) (this.splitStart % this.recordLength);
		if(recordOffset != 0) {
			// move start to next boundary
			super.stream.seek(this.splitStart + recordOffset);
		}
		this.streamPos = this.splitStart + recordOffset;
		this.streamEnd = this.splitStart + this.splitLength;
		this.streamEnd += this.streamEnd % this.recordLength;
		
		// adjust readBufferSize
		this.readBufferSize += this.recordLength - (this.readBufferSize % this.recordLength);
		
		if (this.readBuffer == null || this.readBuffer.length != this.readBufferSize) {
			this.readBuffer = new byte[this.readBufferSize];
		}
		
		this.readBufferLimit = 0;
		this.readBufferPos = 0;
		this.exhausted = false;
		fillReadBuffer();
	}

	/**
	 * {@inheritDoc}
	 * @throws IOException 
	 */
	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		final FileBaseStatistics stats = super.getStatistics(cachedStats);
		return stats == null ? null : 
			new FileBaseStatistics(stats.getLastModificationTime(), stats.getTotalInputSize(), this.recordLength);
	}

	// --------------------------------------------------------------------------------------------
	

	@Override
	public boolean reachedEnd() {
		return this.exhausted;
	}
	

	@Override
	public Record nextRecord(Record reuse) throws IOException {
		// check if read buffer contains another full record
		if (this.readBufferLimit - this.readBufferPos <= 0) {
			// get another buffer
			fillReadBuffer();
			// check if source is exhausted
			if (this.exhausted) {
				return null;
			}
		}
		else if (this.readBufferLimit - this.readBufferPos < this.recordLength) {
			throw new IOException("Unable to read full record");
		}
		
		boolean val = readBytes(reuse, this.readBuffer, this.readBufferPos);
		
		this.readBufferPos += this.recordLength;
		if (this.readBufferPos >= this.readBufferLimit) {
			fillReadBuffer();
		}
		return val ? reuse : null;
	}
	
	/**
	 * Fills the next read buffer from the file stream.
	 * 
	 * @throws IOException
	 */
	private void fillReadBuffer() throws IOException {
		// special case for compressed files.
		if(splitLength == FileInputFormat.READ_WHOLE_SPLIT_FLAG) {
			int read = this.stream.read(this.readBuffer, 0, this.readBufferSize);
			if (read == -1) {
				exhausted = true;
			} else {
				this.streamPos += read;
				this.readBufferPos = 0;
				this.readBufferLimit = read;
			}
			return;
		}
		
		int toRead = (int) Math.min(this.streamEnd - this.streamPos, this.readBufferSize);
		if (toRead <= 0) {
			this.exhausted = true;
			return;
		}
		
		// fill read buffer
		int read = this.stream.read(this.readBuffer, 0, toRead);
		
		if (read <= 0) {
			this.exhausted = true;
		} else {
			this.streamPos += read;
			this.readBufferPos = 0;
			this.readBufferLimit = read;
		}
	}
}
