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

package eu.stratosphere.pact.common.io.input;

import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;

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
import eu.stratosphere.pact.common.io.statistics.FileBaseStatistics;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Base implementation for fixed length input formats. The parameter 'chunkSize'
 * must be specified using the configure() method.
 * 
 * @author Fabian Flechtmann, Fabian Hueske
 */
public abstract class FixedLengthInputFormat<K extends Key, V extends Value> extends FileInputFormat<K, V> 
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
	private static final int DEFAULT_TARGET_READ_BUFFER_SIZE = 1024 * 1024;
	
	/**
	 * Key and value classes
	 */
	private Class<K> keyClass;
	private Class<V> valueClass;
	
	/**
	 * Buffer to hold a single record
	 */
	private byte[] recordBuffer;
	
	/**
	 * Buffer to read a batch of records from a file 
	 */
	private byte[] readBuffer;
	
	/**
	 * number of bytes to read from the split
	 */
	private long remainingByteToRead;
	
	/**
	 * read position within the read buffer
	 */
	private int readBufferPos;
	
	/**
	 * size of the read buffer
	 */
	private int targetReadBufferSize = DEFAULT_TARGET_READ_BUFFER_SIZE;
	
	/**
	 * fixed length of all records
	 */
	private int recordLength;
	
	/**
	 * Flags to indicate the end of the split
	 */
	private boolean noMoreReadBuffers;
	private boolean noMoreRecordBuffers;
	
	/**
	 * Constructor only sets the key and value classes
	 */
	protected FixedLengthInputFormat()
	{
		this.keyClass = getTemplateType1(getClass());
		this.valueClass = getTemplateType2(getClass());
	}
	
	/**
	 * Parses the content of the byte array into a the given key/value pair. 
	 * If the byte array contains invalid content the record can be skipped by returning <tt>false</tt>.
	 * 
	 * @param pair
	 *        the holder for the key/value pair that is read
	 * @param record
	 *        the serialized key/value pair 
	 * @return returns whether the record was successfully deserialized
	 */
	public abstract boolean readBytes(KeyValuePair<K, V> record, byte[] readBuffer);
	
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
	 * Returns the fixed length of a record.
	 * 
	 * @return the fixed length of a record.
	 */
	public int getRecordLength() {
		return this.recordLength;
	}
	
	/**
	 * Sets the target size of the buffer to be used to read from the file stream. 
	 * The actual size depends on the record length since it is chosen such that records are not split.
	 * This method has only an effect, if it is called before the input format is opened.
	 * 
	 * @param targetReadBufferSize The target size of the read buffer.
	 */
	public void setTargetReadBufferSize(int targetReadBufferSize)
	{
		this.targetReadBufferSize = targetReadBufferSize;
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
		
		// check whether current position is directly at record boundary
		int recordOffset = (int)(super.start % recordLength);
		if(recordOffset != 0) {
			// move start to next boundary
			super.stream.seek(this.start + recordOffset);			
		}

		// compute readBufferSize
		if(recordLength > this.targetReadBufferSize) {
			// read buffer is at least as big as record
			this.readBuffer = new byte[recordLength];
		} else if (this.targetReadBufferSize % recordLength == 0) {
			// target read buffer size is a multiple of record length, so it's ok
			this.readBuffer = new byte[this.targetReadBufferSize];
		} else {
			// extent default read buffer size such that records are not split
			this.readBuffer = new byte[(recordLength - (this.targetReadBufferSize % recordLength)) + this.targetReadBufferSize];
		}
		
		// initialize read buffer
		
		
		
		// initialize record buffer
		this.recordBuffer = new byte[this.recordLength];
		// initialize remaining bytes to read
		this.remainingByteToRead = super.length;
		// initialize read buffer pos
		this.readBufferPos = this.readBuffer.length;
		// initialize end flags
		this.noMoreReadBuffers = false;
		this.noMoreRecordBuffers = false;

	}
	
	/**
	 * Closes the file input stream of the input format.
	 */
	@Override
	public void close() throws IOException
	{
		// close file stream of FileInputFormat
		super.close();
		
		// deallocate read buffer
		this.recordBuffer = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean reachedEnd() 
	{
		return noMoreRecordBuffers;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public KeyValuePair<K, V> createPair()
	{
		try {
			return new KeyValuePair<K, V>(this.keyClass.newInstance(), this.valueClass.newInstance());
		}
		catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
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

			// get the file info and check whether the cached statistics are still
			// valid.
			{
				FileStatus status = fs.getFileStatus(file);

				if (status.isDir()) {
					FileStatus[] fss = fs.listStatus(file);
					files = new ArrayList<FileStatus>(fss.length);
					boolean unmodified = true;

					for (FileStatus s : fss) {
						if (!s.isDir()) {
							files.add(s);
							if (s.getModificationTime() > stats.getLastModificationTime()) {
								stats.setFileModTime(s.getModificationTime());
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

					stats.setFileModTime(modTime);
					
					files = new ArrayList<FileStatus>(1);
					files.add(status);
				}
			}

			stats.setAvgBytesPerRecord(this.recordLength);
			stats.setFileSize(0);
			
			// calculate the whole length
			for (FileStatus s : files) {
				stats.setFileSize(s.getLen());
			}
			
			// sanity check
			if (stats.getTotalInputSize() <= 0) {
				stats.setFileSize(BaseStatistics.UNKNOWN);
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

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IOException
	 */
	@Override
	public boolean nextRecord(KeyValuePair<K,V> record) throws IOException {
		
		if(!noMoreRecordBuffers) {
			// fill record buffer
			this.fillRecordBuffer();
			
			// generate key value pair
			return this.readBytes(record, recordBuffer);
		} else {
			return false;
		}
	}
	
	/**
	 * Fills a record buffer by copying from the read buffer
	 * 	
	 * @throws IOException
	 */
	private void fillRecordBuffer() throws IOException {
		
		// fill read buffer if necessary
		if(this.readBufferPos == this.readBuffer.length) {
			this.fillReadBuffer();
			this.readBufferPos = 0;
		}
		
		// copy record
		System.arraycopy(this.readBuffer, readBufferPos, recordBuffer, 0, this.recordLength);
		
		// adapt read buffer position
		this.readBufferPos+=this.recordLength;
		
		if(noMoreReadBuffers && readBufferPos == this.readBuffer.length) {
			this.noMoreRecordBuffers = true;
		}
	}
	
	/**
	 * Fills the next read buffer from the file stream.
	 * 
	 * @throws IOException
	 */
	private void fillReadBuffer() throws IOException {
		
		// remaining split bytes less than read buffer size
		if(this.remainingByteToRead < this.readBuffer.length) {
			// adapt read buffer size
			int lastReadBufferSize;
			int overflowBytes = (int)this.remainingByteToRead % this.recordLength;
			if(overflowBytes != 0) {
				lastReadBufferSize = (int)this.remainingByteToRead + (this.recordLength - overflowBytes);	
			} else {
				lastReadBufferSize = (int)this.remainingByteToRead;
			}
			this.readBuffer = new byte[lastReadBufferSize];
		}
		
		// fill read buffer 
		int bytesRead = super.stream.read(this.readBuffer);
		
		// check whether read buffer was completely filled
		if(bytesRead != this.readBuffer.length) {
			throw new IOException("Unable to read full record");
		} else {
			// adapt counter
			this.remainingByteToRead -= this.readBuffer.length;
		}
		
		if(this.remainingByteToRead <= 0) {
			this.noMoreReadBuffers = true;
		}
	}
	
}
