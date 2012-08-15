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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.parser.FieldParser;

/**
 * Inputformat to parse ASCII text files and generate PactRecords. 
 * The input file is structured by record delimiters and field delimiters (CSV files are common).
 * Record delimiter separate records from each other ('\n' is common).
 * Field delimiters separate fields within a record. 
 * Record and field delimiters must be configured using the InputFormat {@link Configuration}.
 * 
 * The number of fields to parse must be configured as well.  
 * For each field a {@link FieldParser} must be specified using the {@link RecordInputFormat#FIELD_PARSER_PARAMETER_PREFIX} config key.
 * FieldParsers can be configured by adding config entries to the InputFormat configuration. The InputFormat forwards its configuration to each {@link FieldParser}.
 * 
 * The position within the text record can be configured for each field using the {@link RecordInputFormat#TEXT_POSITION_PARAMETER_PREFIX} config key.
 * Either all text postions must be configured or none. If none is configured, the index of the config key is used.
 * 
 * The position within the {@link PactRecord} can be configured for each field using the {@link RecordInputFormat#RECORD_POSITION_PARAMETER_PREFIX} config key.
 * Either all {@link PactRecord} postions must be configured or none. If none is configured, the index of the config key is used.
 * 
 * @see FieldParser
 * @see Configuration
 * @see PactRecord
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class RecordInputFormat extends DelimitedInputFormat implements OutputSchemaProvider
{
	// -------------------------------------- Constants -------------------------------------------
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(RecordInputFormat.class);
	
	// ------------------------------------- Config Keys ------------------------------------------
	
	public static final String FIELD_DELIMITER_PARAMETER = "recordinformat.delimiter.field";
	
	public static final String NUM_FIELDS_PARAMETER = "recordinformat.field.number";
	
	public static final String FIELD_PARSER_PARAMETER_PREFIX = "recordinformat.field.parser_";
	
	public static final String TEXT_POSITION_PARAMETER_PREFIX = "recordinformat.text.position_";
	
	public static final String RECORD_POSITION_PARAMETER_PREFIX = "recordinformat.record.position_";
	
	public static final String RECORD_DELIMITER_PARAMETER = DelimitedInputFormat.RECORD_DELIMITER;
	
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("rawtypes")
	private FieldParser[] fieldParsers;
	private Value[] fieldValues;
	private int[] recordPositions;
	private int numFields;
		
	private char fieldDelim;
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration config) {
		
		super.configure(config);
		
		// read number of fields to parse
		numFields = config.getInteger(NUM_FIELDS_PARAMETER, -1);
		if (numFields < 1) {
			throw new IllegalArgumentException("Invalid configuration for RecordInputFormat: " +
					"Need to specify number of fields > 0.");
		}
		
		int[] textPosIdx = new int[numFields];
		boolean anyTextPosSet = false;
		boolean allTextPosSet = true;
		int maxTextPos = -1;
		
		// parse text positions
		for (int i = 0; i < numFields; i++)
		{
			int pos = config.getInteger(TEXT_POSITION_PARAMETER_PREFIX + i, -1);
			if (pos == -1) {
				allTextPosSet = false;
				textPosIdx[i] = i;
				maxTextPos = i;
			} else {
				anyTextPosSet = true;
				textPosIdx[i] = pos;
				maxTextPos = pos > maxTextPos ? pos : maxTextPos;
			}
		}
		// check if either none or all text positions have been set
		if(anyTextPosSet && !allTextPosSet) {
			throw new IllegalArgumentException("Invalid configuration for RecordInputFormat: " +
					"Not all text positions set");
		}
		
		int[] recPosIdx = new int[numFields];
		boolean anyRecPosSet = false;
		boolean allRecPosSet = true;
		
		// parse record positions
		for (int i = 0; i < numFields; i++)
		{
			int pos = config.getInteger(RECORD_POSITION_PARAMETER_PREFIX + i, -1);
			if (pos == -1) {
				allRecPosSet = false;
				recPosIdx[i] = i;
			} else {
				anyRecPosSet = true;
				recPosIdx[i] = pos;
			}
		}
		// check if either none or all record positions have been set
		if(anyRecPosSet && !allRecPosSet) {
			throw new IllegalArgumentException("Invalid configuration for RecordInputFormat: " +
					"Not all record positions set");
		}
		
		// init parse and position arrays
		this.fieldParsers = new FieldParser[maxTextPos+1];
		this.fieldValues = new Value[maxTextPos+1];
		this.recordPositions = new int[maxTextPos+1];
		for (int j = 0; j < maxTextPos; j++) 
		{
			fieldParsers[j] = null;
			fieldValues[j] = null;
			recordPositions[j] = -1;
		}
		
		for (int i = 0; i < numFields; i++)
		{
			int pos = textPosIdx[i];
			recordPositions[pos] = recPosIdx[i];
			@SuppressWarnings("unchecked")
			Class<? extends FieldParser<Value>> clazz = (Class<? extends FieldParser<Value>>) config.getClass(FIELD_PARSER_PARAMETER_PREFIX + i, null);
			if (clazz == null) {
				throw new IllegalArgumentException("Invalid configuration for RecordInputFormat: " +
					"No field parser class for parameter " + i);
			}
			
			try {
				fieldParsers[pos] = clazz.newInstance();
			} catch(InstantiationException ie) {
				throw new IllegalArgumentException("Invalid configuration for RecordInputFormat: " +
						"No field parser could not be instanciated for parameter " + i);
			} catch (IllegalAccessException e) {
				throw new IllegalArgumentException("Invalid configuration for RecordInputFormat: " +
						"No field parser could not be instanciated for parameter " + i);
			}
			fieldValues[pos] = fieldParsers[pos].getValue();
		}
		
		final String fieldDelimStr = config.getString(FIELD_DELIMITER_PARAMETER, ",");
		if (fieldDelimStr.length() != 1) {
			throw new IllegalArgumentException("Invalid configuration for RecordInputFormat: " +
					"Field delimiter must be a single character");
		}
		this.fieldDelim = fieldDelimStr.charAt(0);
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[], int)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {

		FieldParser<Value> parser;
		Value val;
		int startPos = offset;
		final int limit = offset + numBytes;
		
		if (bytes[startPos] == this.fieldDelim)
			startPos++;
		
		for (int i = 0; i < this.recordPositions.length; i++) {
			
			// check valid start position
			if (startPos >= limit) {
				return false;
			}
			
			if (this.recordPositions[i] > -1) {
				// parse field
				parser = this.fieldParsers[i];
				val = this.fieldValues[i];
				startPos = parser.parseField(bytes, startPos, limit, this.fieldDelim, val);
				// check parse result
				if (startPos < 0) {
					return false;
				}
				target.setField(this.recordPositions[i], val);
			} else {
				// skip field(s)
				int skipCnt = 1;
				while (this.recordPositions[i + skipCnt] == -1) {
					skipCnt++;
				}
				startPos = skipFields(bytes, startPos, limit, this.fieldDelim, skipCnt);
				if (startPos < 0) {
					return false;
				}
				i += (skipCnt - 1);
			}
		}
		return true;
	}
	
	protected int skipFields(byte[] bytes, int startPos, int limit, char delim, int skipCnt)
	{	
		int i = startPos;
		
		while (i < limit && skipCnt > 0) {
			if (bytes[i] == delim) {
				skipCnt--;
			}
			i++;
		}
		if (skipCnt == 0) {
			return i;
		} else {
			return -1;
		}
	}

	@Override
	public int[] getOutputSchema()
	{
		if (this.recordPositions == null) 
			throw new RuntimeException("RecordInputFormat must be configured before output schema is available");
		
		final int[] outputSchema = new int[this.numFields];
		int j = 0;
		
		for(int i = 0; i < this.recordPositions.length; i++) {
			if (this.recordPositions[i] > 0) {
				outputSchema[j++] = this.recordPositions[i];
			}
		}
		Arrays.sort(outputSchema);
		return outputSchema;
	}
	
	// ============================================================================================
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureRecordFormat(FileDataSource target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * An abstract builder used to set parameters to the input format's configuration in a fluent way.
	 */
	protected static class AbstractConfigBuilder<T> extends DelimitedInputFormat.AbstractConfigBuilder<T>
	{
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
		 * Sets the delimiter that delimits the individual fields in the records textual input representation.
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
		
		public T field(Class<? extends FieldParser<?>> parser, int textPosition) {
			final int numYet = this.config.getInteger(NUM_FIELDS_PARAMETER, 0);
			this.config.setClass(FIELD_PARSER_PARAMETER_PREFIX + numYet, parser);
			this.config.setInteger(TEXT_POSITION_PARAMETER_PREFIX + numYet, textPosition);
			this.config.setInteger(NUM_FIELDS_PARAMETER, numYet + 1);
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