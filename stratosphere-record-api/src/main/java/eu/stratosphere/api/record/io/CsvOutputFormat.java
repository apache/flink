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


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.Value;


/**
 * This is an OutputFormat to serialize {@link PactRecord}s to text. 
 * The output is structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common).
 * Field delimiters separate fields within a record. 
 * Record and field delimiters can be configured using the CsvOutputFormat {@link Configuration}.
 * 
 * The number of fields to serialize must be configured as well.  
 * For each field the type of the {@link Value} must be specified using the {@link CsvOutputFormat#FIELD_TYPE_PARAMETER_PREFIX} config key
 * and an index running from 0 to the number of fields.
 *  
 * The position within the {@link PactRecord} can be configured for each field using the 
 * {@link CsvOutputFormat#RECORD_POSITION_PARAMETER_PREFIX} config key.
 * Either all {@link PactRecord} positions must be configured or none. If none is configured, the index of the config key is used.
 * 
 * @see Value
 * @see Configuration
 * @see PactRecord
 */
public class CsvOutputFormat extends FileOutputFormat {
	private static final long serialVersionUID = 1L;
	
	public static final String RECORD_DELIMITER_PARAMETER = "pact.output.record.delimiter";
	
	private static final String RECORD_DELIMITER_ENCODING = "pact.output.record.delimiter-encoding";
	
	public static final String FIELD_DELIMITER_PARAMETER = "pact.output.record.field-delimiter";
	
	public static final String NUM_FIELDS_PARAMETER = "pact.output.record.num-fields";
	
	public static final String FIELD_TYPE_PARAMETER_PREFIX = "pact.output.record.type_";
	
	public static final String RECORD_POSITION_PARAMETER_PREFIX = "pact.output.record.position_";
	
	public static final String LENIENT_PARSING = "pact.output.record.lenient";
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(CsvOutputFormat.class);
	
	// --------------------------------------------------------------------------------------------
	
	private int numFields;
	
	private Class<? extends Value>[] classes;
	
	private int[] recordPositions;

	private Writer wrt;
	
	private String fieldDelimiter;
	
	private String recordDelimiter;
	
	private String charsetName;
	
	private boolean lenient;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Configuration parameters)
	{
		super.configure(parameters);
		
		this.numFields = parameters.getInteger(NUM_FIELDS_PARAMETER, -1);
		if (this.numFields < 1) {
			throw new IllegalArgumentException("Invalid configuration for CsvOutputFormat: " +
					"Need to specify number of fields > 0.");
		}
		
		@SuppressWarnings("unchecked")
		Class<Value>[] arr = new Class[this.numFields];
		this.classes = arr;
		
		for (int i = 0; i < this.numFields; i++)
		{
			@SuppressWarnings("unchecked")
			Class<? extends Value> clazz = (Class<? extends Value>) parameters.getClass(FIELD_TYPE_PARAMETER_PREFIX + i, null);
			if (clazz == null) {
				throw new IllegalArgumentException("Invalid configuration for CsvOutputFormat: " +
					"No type class for parameter " + i);
			}
			
			this.classes[i] = clazz;
		}
		
		this.recordPositions = new int[this.numFields];
		boolean anyRecordPosDefined = false;
		boolean allRecordPosDefined = true;
		
		for(int i = 0; i < this.numFields; i++) {
			
			int pos = parameters.getInteger(RECORD_POSITION_PARAMETER_PREFIX + i, Integer.MIN_VALUE);
			
			if(pos != Integer.MIN_VALUE) {
				anyRecordPosDefined = true;
				
				if(pos < 0) {
					throw new IllegalArgumentException("Invalid configuration for CsvOutputFormat: " +
							"Invalid record position for parameter " + i);
				}

				this.recordPositions[i] = pos;
				
			} else {
				allRecordPosDefined = false;
				
				this.recordPositions[i] = i;
			}
		}
		
		if(anyRecordPosDefined && !allRecordPosDefined) {
			throw new IllegalArgumentException("Invalid configuration for CsvOutputFormat: " +
					"Either none or all record positions must be defined.");
		}
		
		this.recordDelimiter = parameters.getString(RECORD_DELIMITER_PARAMETER, AbstractConfigBuilder.NEWLINE_DELIMITER);
		if (this.recordDelimiter == null) {
			throw new IllegalArgumentException("The delimiter in the DelimitedOutputFormat must not be null.");
		}
		this.charsetName = parameters.getString(RECORD_DELIMITER_ENCODING, null);
		this.fieldDelimiter = parameters.getString(FIELD_DELIMITER_PARAMETER, "|");
		this.lenient = parameters.getBoolean(LENIENT_PARSING, false);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.api.record.io.FileOutputFormat#open(int)
	 */
	@Override
	public void open(int taskNumber) throws IOException
	{
		super.open(taskNumber);
		this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096)) :
				new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.api.record.io.FileOutputFormat#close()
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
	public void writeRecord(PactRecord record) throws IOException
	{
		int numRecFields = record.getNumFields();
		int readPos;
		
		for(int i=0; i<this.numFields; i++) {
			
			readPos = this.recordPositions[i];
			
			if(readPos < numRecFields) {
			
				Value v = record.getField(this.recordPositions[i], this.classes[i]);
				
				if(v != null) {
					if (i != 0)
						this.wrt.write(this.fieldDelimiter);
					this.wrt.write(v.toString());
					
				} else {
					if(this.lenient) {
						if (i != 0)
							this.wrt.write(this.fieldDelimiter);
					} else {
						throw new RuntimeException("Cannot serialize record with <null> value at position: "+readPos);
					}
				}
				
			} else {
				if(this.lenient) {
					if (i != 0)
						this.wrt.write(this.fieldDelimiter);
				} else {
					throw new RuntimeException("Cannot serialize record with out field at position: "+readPos);
				}
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
	public static ConfigBuilder configureRecordFormat(FileDataSink target) {
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
			this.config.setString(RECORD_DELIMITER_PARAMETER, delimiter);
			this.config.setString(RECORD_DELIMITER_ENCODING, charsetName);
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
		 * Adds a field of the record to be serialized to the output. The field at the given position will
		 * be interpreted as the type represented by the given class. The types {@link Object#toString()} method
		 * will be invoked to create a textual representation. 
		 * 
		 * @param type The type of the field.
		 * @param recordPosition The position in the record.
		 * @return The builder itself.
		 */
		public T field(Class<? extends Value> type, int recordPosition) {
			final int numYet = this.config.getInteger(NUM_FIELDS_PARAMETER, 0);
			this.config.setClass(FIELD_TYPE_PARAMETER_PREFIX + numYet, type);
			this.config.setInteger(RECORD_POSITION_PARAMETER_PREFIX + numYet, recordPosition);
			this.config.setInteger(NUM_FIELDS_PARAMETER, numYet + 1);
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
