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

package eu.stratosphere.api.record.io;


import java.io.IOException;
import java.io.UnsupportedEncodingException;

import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.PactRecord;


/**
 * The base class for output formats that serialize their records into a delimited sequence.
 */
public abstract class DelimitedOutputFormat extends FileOutputFormat {
	private static final long serialVersionUID = 1L;

	/**
	 * The configuration key for the entry that defines the record delimiter.
	 */
	public static final String RECORD_DELIMITER = "pact.output.delimited.delimiter";

	/**
	 * The configuration key to set the record delimiter encoding.
	 */
	private static final String RECORD_DELIMITER_ENCODING = "pact.output.delimited.delimiter-encoding";
	
	/**
	 * The configuration key for the entry that defines the write-buffer size.
	 */
	public static final String WRITE_BUFFER_SIZE = "pact.output.delimited.buffersize";
	
	/**
	 * The default write-buffer size. 64 KiByte. 
	 */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 64 * 1024;
	
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
	 * @see eu.stratosphere.api.record.io.FileOutputFormat#configure(eu.stratosphere.configuration.Configuration)
	 */
	public void configure(Configuration config)
	{
		super.configure(config);
		
		final String delim = config.getString(RECORD_DELIMITER, "\n");
		final String charsetName = config.getString(RECORD_DELIMITER_ENCODING, null);		
		if (delim == null) {
			throw new IllegalArgumentException("The delimiter in the DelimitedOutputFormat must not be null.");
		}
		try {
			this.delimiter = charsetName == null ? delim.getBytes() : delim.getBytes(charsetName);
		} catch (UnsupportedEncodingException useex) {
			throw new IllegalArgumentException("The charset with the name '" + charsetName + 
				"' is not supported on this TaskManager instance.", useex);
		}
		
		this.bufferSize = config.getInteger(WRITE_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE);
		if (this.bufferSize < MIN_WRITE_BUFFER_SIZE) {
			throw new IllegalArgumentException("The write buffer size must not be less than " + MIN_WRITE_BUFFER_SIZE
				+ " bytes.");
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.api.record.io.FileOutputFormat#open(int)
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
	 * @see eu.stratosphere.api.record.io.FileOutputFormat#close()
	 */
	@Override
	public void close() throws IOException {
		if (this.stream != null) {
			this.stream.write(this.buffer, 0, this.pos);
		}
		
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

	// ============================================================================================
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureDelimitedFormat(FileDataSink target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	protected static abstract class AbstractConfigBuilder<T> extends FileOutputFormat.AbstractConfigBuilder<T>
	{
		private static final String NEWLINE_DELIMITER = "\n";
		
		// --------------------------------------------------------------------
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param config The configuration into which the parameters will be written.
		 */
		protected AbstractConfigBuilder(Configuration config) {
			super(config);
		}
		
		// --------------------------------------------------------------------
		
		/**
		 * Sets the delimiter to be a single character, namely the given one. The character must be within
		 * the value range <code>0</code> to <code>127</code>.
		 * 
		 * @param delimiter The delimiter character.
		 * @return The builder itself.
		 */
		public T recordDelimiter(char delimiter) {
			if (delimiter == '\n') {
				this.config.setString(RECORD_DELIMITER, NEWLINE_DELIMITER);
			} else {
				this.config.setString(RECORD_DELIMITER, String.valueOf(delimiter));
			}
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the delimiter to be the given string. The string will be converted to bytes for more efficient
		 * comparison during input parsing. The conversion will be done using the platforms default charset.
		 * 
		 * @param delimiter The delimiter string.
		 * @return The builder itself.
		 */
		public T recordDelimiter(String delimiter) {
			this.config.setString(RECORD_DELIMITER, delimiter);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the delimiter to be the given string. The string will be converted to bytes for more efficient
		 * comparison during input parsing. The conversion will be done using the charset with the given name.
		 * The charset must be available on the processing nodes, otherwise an exception will be raised at
		 * runtime.
		 * 
		 * @param delimiter The delimiter string.
		 * @param charsetName The name of the encoding character set.
		 * @return The builder itself.
		 */
		public T recordDelimiter(String delimiter, String charsetName) {
			this.config.setString(RECORD_DELIMITER, delimiter);
			this.config.setString(RECORD_DELIMITER_ENCODING, charsetName);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the size of the write buffer.
		 * 
		 * @param sizeInBytes The size of the write buffer in bytes.
		 * @return The builder itself.
		 */
		public T writeBufferSize(int sizeInBytes) {
			this.config.setInteger(WRITE_BUFFER_SIZE, sizeInBytes);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
	}
	
	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	public static class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder>
	{
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected ConfigBuilder(Configuration targetConfig) {
			super(targetConfig);
		}
		
	}
}
