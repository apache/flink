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

package eu.stratosphere.pact.common.io;


import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.PactRecord;


/**
 * 
 */
public abstract class DelimitedOutputFormat extends FileOutputFormat
{
	/**
	 * The configuration key for the entry that defines the record delimiter.
	 */
	public static final String RECORD_DELIMITER = "pact.output.delimited.delimiter";

	/**
	 * The configuration key for the entry that defines the write-buffer size.
	 */
	public static final String WRITE_BUFFER_SIZE = "pact.output.delimited.buffersize";
	
	/**
	 * The default write-buffer size. 1 MiByte. 
	 */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 1024;
	
	/**
	 * The minimal write-buffer size, 1 KiByte.
	 */
	private static final int MIN_WRITE_BUFFER_SIZE = 1024;
	
	// --------------------------------------------------------------------------------------------
	
	private byte[] delimiter;
	
	private byte[] buffer;
	
	private byte[] targetArray = new byte[64];
	
	private int pos;
	
	private int bufferSize;
	
	// --------------------------------------------------------------------------------------------
	
	
	/**
	 * Calls the super classes to configure themselves and reads the config parameters for the delimiter and
	 * the write buffer size.
	 * 
	 *  @param config The configuration to read the parameters from.
	 *  
	 * @see eu.stratosphere.pact.common.io.FileOutputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	public void configure(Configuration config)
	{
		super.configure(config);
		
		String delim = config.getString(RECORD_DELIMITER, "\n");
		if (delim == null) {
			throw new IllegalArgumentException("The delimiter in the DelimitedOutputFormat must not be null.");
		}
		this.delimiter = delim.getBytes();
		
		this.bufferSize = config.getInteger(WRITE_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE);
		if (this.bufferSize < MIN_WRITE_BUFFER_SIZE) {
			throw new IllegalArgumentException("The write buffer size must not be less than " + MIN_WRITE_BUFFER_SIZE
				+ " bytes.");
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileOutputFormat#open(int)
	 */
	public void open(int taskNumber) throws IOException
	{
		super.open(taskNumber);
		
		if (this.buffer == null) {
			this.buffer = new byte[this.bufferSize];
		}
		if (this.targetArray == null) {
			this.targetArray = new byte[64];
		}
		this.pos = 0;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileOutputFormat#close()
	 */
	@Override
	public void close() throws IOException
	{
		this.stream.write(this.buffer, 0, this.pos);
		
		// close file stream
		super.close();
	}

	/**
	 * This method is called for every record so serialize itself into the given target array. The method should
	 * return the number of bytes occupied in the target array. If the target array is not large enough, a negative 
	 * value should be returned.
	 * <p>
	 * The absolute value of the returned integer can be given as a hint how large an array is required. The array is
	 * resized to the return value's absolute value, if that is larger than the current array size. Otherwise, the
	 * array size is simply doubled.
	 * 
	 * @param rec The record to be serialized.
	 * @param target The array to serialize the record into.
	 * @return The length of the serialized contents, or a negative value, indicating that the array is too small.
	 * 
	 * @throws Exception If the user code produces an exception that prevents processing the record, it should
	 *                   throw it such that the engine recognizes the situation as a fault.
	 */
	public abstract int serializeRecord(PactRecord rec, byte[] target) throws Exception;
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#writeRecord(eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public void writeRecord(PactRecord record) throws IOException
	{
		int size;
		try {
			while ((size = serializeRecord(record, this.targetArray)) < 0) {
				if (-size > this.targetArray.length) {
					this.targetArray = new byte[-size];
				}
				else {
					this.targetArray = new byte[this.targetArray.length * 2];
				}
			}
		}
		catch (Exception ex) {
			throw new IOException("Error while serializing the record to bytes: " + ex.getMessage(), ex);
		}
		
		if (this.bufferSize - this.pos > size + this.delimiter.length) {
			System.arraycopy(this.targetArray, 0, this.buffer, this.pos, size);
			System.arraycopy(this.delimiter, 0, this.buffer, pos + size, this.delimiter.length);
			pos += size + this.delimiter.length;
		}
		else {
			// copy the target array (piecewise)
			int off = 0;
			while (off < size) {
				int toCopy = Math.min(size - off, this.bufferSize - this.pos);
				System.arraycopy(this.targetArray, off, this.buffer, this.pos, toCopy);

				off += toCopy;
				this.pos += toCopy;
				if (this.pos == this.bufferSize) {
					this.pos = 0;
					this.stream.write(this.buffer, 0, this.bufferSize);
				}
			}
			
			// copy the delimiter (piecewise)
			off = 0;
			while (off < this.delimiter.length) {
				int toCopy = Math.min(this.delimiter.length - off, this.bufferSize - this.pos);
				System.arraycopy(this.delimiter, off, this.buffer, this.pos, toCopy);

				off += toCopy;
				this.pos += toCopy;
				if (this.pos == this.bufferSize) {
					this.pos = 0;
					this.stream.write(this.buffer, 0, this.bufferSize);
				}
			}
		}
	}

}
