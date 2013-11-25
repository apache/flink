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

package eu.stratosphere.pact.array.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.generic.io.FileOutputFormat;

/**
 * 
 */
public abstract class ArrayOutputFormat extends FileOutputFormat<Value[]> implements ArrayModelOutputFormat
{
	private static final long serialVersionUID = 1L;

	private static final String RECORD_DELIMITER_PARAMETER = "pact.output.array.delimiter";
	
	private static final String FIELD_DELIMITER_PARAMETER = "pact.output.array.field-delimiter";
	
	private static final String ENCODING_PARAMETER = "pact.output.array.encoding";
	
	private static final String LENIENT_PARSING = "pact.output.array.lenient";
	
	// --------------------------------------------------------------------------------------------

	private OutputStreamWriter wrt;
	
	private String fieldDelimiter;
	
	private String recordDelimiter;
	
	private String charsetName;
	
	private boolean lenient;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		
		this.recordDelimiter = parameters.getString(RECORD_DELIMITER_PARAMETER, AbstractConfigBuilder.NEWLINE_DELIMITER);
		if (this.recordDelimiter == null) {
			throw new IllegalArgumentException("The delimiter in the DelimitedOutputFormat must not be null.");
		}
		this.charsetName = parameters.getString(ENCODING_PARAMETER, null);
		this.fieldDelimiter = parameters.getString(FIELD_DELIMITER_PARAMETER, "|");
		this.lenient = parameters.getBoolean(LENIENT_PARSING, false);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileOutputFormat#open(int)
	 */
	@Override
	public void open(int taskNumber) throws IOException
	{
		super.open(taskNumber);
		this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096)) :
				new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileOutputFormat#close()
	 */
	@Override
	public void close() throws IOException
	{
		if(wrt != null) {
			this.wrt.close();
		}
		super.close();
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#writeRecord(eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public void writeRecord(Value[] record) throws IOException {
		for (int i = 0; i < record.length; i++) {
			final Value v = record[i];
			if (v != null) {
				if (i != 0) {
					this.wrt.write(this.fieldDelimiter);
				}
				this.wrt.write(v.toString());
			} else if (this.lenient) {
				if (i != 0) {
					this.wrt.write(this.fieldDelimiter);
				}
			} else {
				throw new RuntimeException("Cannot serialize record with <null> value at position: " + i);
			}
		}
		
		// add the record delimiter
		this.wrt.write(this.recordDelimiter);
	}

	// ============================================================================================
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureArrayFormat(FileDataSink target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * Abstract builder used to set parameters to the input format's configuration in a fluent way.
	 */
	protected static abstract class AbstractConfigBuilder<T> extends FileOutputFormat.AbstractConfigBuilder<T>
	{
		private static final String NEWLINE_DELIMITER = "\n";
		
		// --------------------------------------------------------------------
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
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
				this.config.setString(RECORD_DELIMITER_PARAMETER, NEWLINE_DELIMITER);
			} else {
				this.config.setString(RECORD_DELIMITER_PARAMETER, String.valueOf(delimiter));
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
			this.config.setString(RECORD_DELIMITER_PARAMETER, delimiter);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the charset that will be used to encode the strings binary. 
		 * The charset must be available on the processing nodes, otherwise an exception will be raised at
		 * runtime.
		 * 
		 * @param charsetName The name of the encoding character set.
		 * @return The builder itself.
		 */
		public T encoding(String charsetName) {
			this.config.setString(ENCODING_PARAMETER, charsetName);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the delimiter that delimits the individual fields in the records textual output representation.
		 * 
		 * @param delimiter The character to be used as a field delimiter.
		 * @return The builder itself.
		 */
		public T fieldDelimiter(char delimiter) {
			this.config.setString(FIELD_DELIMITER_PARAMETER, String.valueOf(delimiter));
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the leniency for the serializer. A lenient serializer simply skips missing fields and null
		 * fields in the record, while a non lenient one throws an exception.
		 * 
		 * @param lenient True, if the serializer should be lenient, false otherwise.
		 * @return The builder itself.
		 */
		public T lenient(boolean lenient) {
			this.config.setBoolean(LENIENT_PARSING, lenient);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
	}
	
	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	public static final class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder>
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
