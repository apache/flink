/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.io;


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;


/**
 * 
 */
public abstract class FixedLengthInputFormat extends FileInputFormat 
{
	/**
	 * The config parameter which defines the fixed length of a record.
	 */
	public static final String RECORDLENGTH_PARAMETER_KEY = "pact.input.recordLength";
	
	/**
	 * The log.
	 */
	private static final Log LOG = LogFactory.getLog(FixedLengthInputFormat.class);
	
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
	protected FixedLengthInputFormat()
	{}
	
	/**
	 * Reads a record out of the given buffer. This operation always consumes the standard number of
	 * bytes, regardless of whether the produced record was valid.
	 * 
	 * @param target The target PactRecord
	 * @param buffer The buffer containing the binary data.
	 * @param startPos The start position in the byte array.
	 * @return True, is the record is valid, false otherwise.
	 */
	public abstract boolean readBytes(PactRecord target, byte[] buffer, int startPos);
	
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
	public int getReadBufferSize()
	{
		return this.readBuffer.length;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Configuration parameters)
	{
		// pass parameters to FileInputFormat
		super.configure(parameters);

		// read own parameters
		this.recordLength = parameters.getInteger(RECORDLENGTH_PARAMETER_KEY, 0);
		if (recordLength < 1) {
			throw new IllegalArgumentException("The record length parameter must be set and larger than 0.");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws IOException
	 */
	@Override
	public void open(FileInputSplit split) throws IOException
	{
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
		this.readBufferSize += (this.recordLength - (this.readBufferSize % this.recordLength));
		
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
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		
		// check the cache
		FileBaseStatistics stats = null;
		
		if (cachedStatistics != null && cachedStatistics instanceof FileBaseStatistics) {
			stats = (FileBaseStatistics) cachedStatistics;
		}
		else {
			stats = new FileBaseStatistics(-1, BaseStatistics.UNKNOWN, BaseStatistics.UNKNOWN);
		}
		
		try {
			final Path file = this.filePath;
			final URI uri = file.toUri();

			// get the filesystem
			final FileSystem fs = FileSystem.get(uri);
			List<FileStatus> files = null;

			// get the file info and check whether the cached statistics are still valid.
			{
				FileStatus status = fs.getFileStatus(file);

				if (status.isDir()) {
					FileStatus[] fss = fs.listStatus(file);
					files = new ArrayList<FileStatus>(fss.length);
					boolean unmodified = stats.avgBytesPerRecord == (float) this.recordLength;

					for (FileStatus s : fss) {
						if (!s.isDir()) {
							files.add(s);
							if (s.getModificationTime() > stats.getLastModificationTime()) {
								stats.fileModTime = s.getModificationTime();
								unmodified = false;
							}
						}
					}

					if (unmodified) {
						return stats;
					}
				}
				else {
					// check if the statistics are up to date
					long modTime = status.getModificationTime();	
					if (stats.getLastModificationTime() == modTime) {
						return stats;
					}

					stats.fileModTime = modTime;
					
					files = new ArrayList<FileStatus>(1);
					files.add(status);
				}
			}

			stats.avgBytesPerRecord = this.recordLength;
			stats.fileSize = 0;
			
			// calculate the whole length
			for (FileStatus s : files) {
				stats.fileSize += s.getLen();
			}
			
			// sanity check
			if (stats.getTotalInputSize() <= 0) {
				stats.fileModTime = BaseStatistics.UNKNOWN;
				return stats;
			}
		}
		catch (IOException ioex) {
			if (LOG.isWarnEnabled())
				LOG.warn("Could not determine complete statistics for file '" + filePath + "' due to an io error: "
						+ ioex.getMessage());
		}
		catch (Throwable t) {
			if (LOG.isErrorEnabled())
				LOG.error("Unexpected problen while getting the file statistics for file '" + filePath + "': "
						+ t.getMessage(), t);
		}

		return stats;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean reachedEnd() 
	{
		return this.exhausted;
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @throws IOException
	 */
	@Override
	public boolean nextRecord(PactRecord record) throws IOException
	{
		// check if read buffer contains another full record
		if((this.readBufferLimit - this.readBufferPos) == 0) {
			// get another buffer
			fillReadBuffer();
			// check if source is exhausted
			if(this.exhausted)
				return false;
		} else if((this.readBufferLimit - this.readBufferPos) < this.recordLength) {
			throw new IOException("Unable to read full record");
		}
		
		boolean val = readBytes(record, this.readBuffer, this.readBufferPos);
		
		this.readBufferPos += this.recordLength;
		if (this.readBufferPos >= this.readBufferLimit) {
			fillReadBuffer();
		}
		return val;
	}
	
	/**
	 * Fills the next read buffer from the file stream.
	 * 
	 * @throws IOException
	 */
	private void fillReadBuffer() throws IOException
	{
		
		int toRead = (int) Math.min(this.streamEnd - this.streamPos, this.readBufferSize);
		if (toRead <= 0) {
			this.exhausted = true;
			return;
		}
		
		// fill read buffer
		int read = this.stream.read(this.readBuffer, 0, toRead);
		
		if (read <= 0) {
			this.exhausted = true;
		}
		else {
			this.streamPos += read;
			this.readBufferPos = 0;
			this.readBufferLimit = read;
		}
	}
	
}
