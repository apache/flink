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

import org.apache.flink.api.common.io.GenericCsvInputFormat;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.operators.CompilerHints;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.types.parser.FieldParser;

import com.google.common.base.Preconditions;

/**
 * Input format to parse text files and generate Records. 
 * The input file is structured by record delimiters and field delimiters (CSV files are common).
 * Record delimiter separate records from each other ('\n' is common).
 * Field delimiters separate fields within a record. 
 * Record and field delimiters must be configured using the InputFormat {@link Configuration}.
 * 
 * The number of fields to parse must be configured as well.  
 * For each field a data type must be specified using the {@link CsvInputFormat#FIELD_TYPE_PARAMETER_PREFIX} config key.
 * 
 * The position within the text record can be configured for each field using the {@link CsvInputFormat#TEXT_POSITION_PARAMETER_PREFIX} config key.
 * Either all text positions must be configured or none. If none is configured, the index of the config key is used.
 * The position of a value within the {@link Record} is the index of the config key.
 * 
 * @see Configuration
 * @see Record
 */
public class CsvInputFormat extends GenericCsvInputFormat<Record> {
	
	private static final long serialVersionUID = 1L;
	
	private transient Value[] parsedValues;
	
	private int[] targetPositions = new int[0];

	private boolean configured = false;
	
	//To speed up readRecord processing. Used to find windows line endings.
	//It is set when open so that readRecord does not have to evaluate it
	private boolean lineDelimiterIsLinebreak = false;
	
	// --------------------------------------------------------------------------------------------
	//  Constructors and getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------
	
	public CsvInputFormat() {
		super();
	}
	
	public CsvInputFormat(char fieldDelimiter) {
		super();
		setFieldDelimiter(fieldDelimiter);
	}
	
	public CsvInputFormat(Class<? extends Value> ... fields) {
		super();
		setFieldTypes(fields);
	}
	
	public CsvInputFormat(char fieldDelimiter, Class<? extends Value> ... fields) {
		super();
		setFieldDelimiter(fieldDelimiter);
		setFieldTypes(fields);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void setFieldTypesArray(Class<? extends Value>[] fieldTypes) {
		setFieldTypes(fieldTypes);
	}

	public void setFieldTypes(Class<? extends Value> ... fieldTypes) {
		if (fieldTypes == null) {
			throw new IllegalArgumentException("Field types must not be null.");
		}
		
		// sanity check
		for (Class<? extends Value> type : fieldTypes) {
			if (type != null && !Value.class.isAssignableFrom(type)) {
				throw new IllegalArgumentException("The types must be subclasses if " + Value.class.getName());
			}
		}
		
		setFieldTypesGeneric(fieldTypes);
	}

	public void setFields(int[] sourceFieldIndices, Class<? extends Value>[] fieldTypes) {
		Preconditions.checkNotNull(fieldTypes);
		
		// sanity check
		for (Class<? extends Value> type : fieldTypes) {
			if (!Value.class.isAssignableFrom(type)) {
				throw new IllegalArgumentException("The types must be subclasses if " + Value.class.getName());
			}
		}
		
		setFieldsGeneric(sourceFieldIndices, fieldTypes);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Pre-flight: Configuration
	// --------------------------------------------------------------------------------------------

	@Override
	public void configure(Configuration config) {
		super.configure(config);

		if (configured) {
			return;
		}
		
		final String fieldDelimStr = config.getString(FIELD_DELIMITER_PARAMETER, null);
		if (fieldDelimStr != null) {
			if (fieldDelimStr.length() != 1) {
				throw new IllegalArgumentException("Invalid configuration for CsvInputFormat: " +
						"Field delimiter must be a single character");
			} else {
				setFieldDelimiter(fieldDelimStr.charAt(0));
			}
		}
		
		// read number of field configured via configuration
		int numConfigFields = config.getInteger(NUM_FIELDS_PARAMETER, -1);
		if (numConfigFields != -1) {
			if (numConfigFields <= 0) {
				throw new IllegalConfigurationException("The number of fields for the CsvInputFormat is invalid.");
			}
			
			if (getNumberOfNonNullFields() > 0) {
				throw new IllegalConfigurationException("Mixing configuration via instance parameters and config parameters is not possible.");
			}
		
			int[] textPosIdx = new int[numConfigFields];
			boolean anyTextPosSet = false;
			boolean allTextPosSet = true;
			int maxTextPos = -1;
			
			// parse text positions
			for (int i = 0; i < numConfigFields; i++) {
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
			if (anyTextPosSet && !allTextPosSet) {
				throw new IllegalArgumentException("Invalid configuration for CsvInputFormat: " +
						"Not all text positions set");
			}
			
			// init the array of types to be set. unify the types from the config 
			// with the types array set on the instance
			
			// make sure we have a sufficiently large types array
			@SuppressWarnings("unchecked")
			Class<? extends Value>[] types = (Class<? extends Value>[]) new Class[maxTextPos+1];
			int[] targetPos = new int[maxTextPos+1];
			
			// set the fields
			for (int i = 0; i < numConfigFields; i++) {
				int pos = textPosIdx[i];
				
				Class<? extends Value> clazz = config.getClass(FIELD_TYPE_PARAMETER_PREFIX + i, null).asSubclass(Value.class);
				if (clazz == null) {
					throw new IllegalConfigurationException("Invalid configuration for CsvInputFormat: " +
						"No field parser class for parameter " + i);
				}
				
				types[pos] = clazz;
				targetPos[pos] = i;
			}
			
			// update the field types
			setFieldTypes(types);
			
			// make a dense target pos array
			this.targetPositions = new int[numConfigFields];
			for (int i = 0, k = 0; i < targetPos.length; i++) {
				if (types[i] != null) {
					this.targetPositions[k++] = targetPos[i];
				}
			}
		}
		else {
			// not configured via config parameters
			if (this.targetPositions.length == 0) {
				this.targetPositions = new int[getNumberOfNonNullFields()];
				for (int i = 0; i < this.targetPositions.length; i++) {
					this.targetPositions[i] = i;
				}
			}
		}
		
		if (getNumberOfNonNullFields() == 0) {
			throw new IllegalConfigurationException("No fields configured in the CsvInputFormat.");
		}

		this.configured = true;
	}
	
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		
		@SuppressWarnings("unchecked")
		FieldParser<Value>[] fieldParsers = (FieldParser<Value>[]) getFieldParsers();
		
		// create the value holders
		this.parsedValues = new Value[fieldParsers.length];
		for (int i = 0; i < fieldParsers.length; i++) {
			this.parsedValues[i] = fieldParsers[i].createValue();
		}
		
		//left to right evaluation makes access [0] okay
		//this marker is used to fasten up readRecord, so that it doesn't have to check each call if the line ending is set to default
		if(this.getDelimiter().length == 1 && this.getDelimiter()[0] == '\n' ) {
					this.lineDelimiterIsLinebreak = true;
		}
	}
	
	@Override
	public Record readRecord(Record reuse, byte[] bytes, int offset, int numBytes) throws ParseException {
		/*
		 * Fix to support windows line endings in CSVInputFiles with standard delimiter setup = \n
		 */
		//Find windows end line, so find chariage return before the newline 
		if(this.lineDelimiterIsLinebreak == true && bytes[offset + numBytes -1] == '\r') {
			//reduce the number of bytes so that the Carriage return is not taken as data
			numBytes--;
		}
		
		if (parseRecord(parsedValues, bytes, offset, numBytes)) {
			// valid parse, map values into pact record
			for (int i = 0; i < parsedValues.length; i++) {
				reuse.setField(targetPositions[i], parsedValues[i]);
			}
			return reuse;
		} else {
			return null;
		}
	}
	
	// ============================================================================================
	//  Parameterization via configuration
	// ============================================================================================
	
	// ------------------------------------- Config Keys ------------------------------------------
	
	private static final String FIELD_DELIMITER_PARAMETER = "recordinformat.delimiter.field";
	
	private static final String NUM_FIELDS_PARAMETER = "recordinformat.field.number";
	
	private static final String FIELD_TYPE_PARAMETER_PREFIX = "recordinformat.field.type_";
	
	private static final String TEXT_POSITION_PARAMETER_PREFIX = "recordinformat.text.position_";
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureRecordFormat(FileDataSource target) {
		return new ConfigBuilder(target, target.getParameters());
	}
	
	/**
	 * An abstract builder used to set parameters to the input format's configuration in a fluent way.
	 */
	protected static class AbstractConfigBuilder<T> extends DelimitedInputFormat.AbstractConfigBuilder<T> {
		
		protected final RecordFormatCompilerHints hints;
		
		/**
		 * Creates a new builder for the given configuration.
		 *
		 * @param contract The contract from which the the compiler hints are used. 
		 *                 If contract is null, new compiler hints are generated.  
		 * @param config The configuration into which the parameters will be written.
		 */
		protected AbstractConfigBuilder(Operator<?> contract, Configuration config) {
			super(config);
			
			if (contract != null) {
				this.hints = new RecordFormatCompilerHints(contract.getCompilerHints());
				
				// initialize with 2 bytes length for the header (its actually 3, but one is skipped on the first field
				this.hints.addWidthRecordFormat(2);
			}
			else {
				this.hints = new RecordFormatCompilerHints(new CompilerHints());
			}
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
		
		public T field(Class<? extends Value> type, int textPosition) {
			return field(type, textPosition, Float.NEGATIVE_INFINITY);

		}
		
		public T field(Class<? extends Value> type, int textPosition, float avgLen) {
			// register field
			final int numYet = this.config.getInteger(NUM_FIELDS_PARAMETER, 0);
			this.config.setClass(FIELD_TYPE_PARAMETER_PREFIX + numYet, type);
			this.config.setInteger(TEXT_POSITION_PARAMETER_PREFIX + numYet, textPosition);
			this.config.setInteger(NUM_FIELDS_PARAMETER, numYet + 1);
			
			// register length
			if (avgLen == Float.NEGATIVE_INFINITY) {
				if (type == IntValue.class) {
					avgLen = 4f;
				} else if (type == DoubleValue.class || type == LongValue.class) {
					avgLen = 8f;
				}
			}
			
			if (avgLen != Float.NEGATIVE_INFINITY) {
				// add the len, plus one byte for the offset coding
				this.hints.addWidthRecordFormat(avgLen + 1);
			}
			
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
	}
	
	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	public static class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder> {
		
		protected ConfigBuilder(Operator<?> target, Configuration targetConfig) {
			super(target, targetConfig);
		}
	}
	
	private static final class RecordFormatCompilerHints extends CompilerHints {
		
		private float width = 0.0f;
		
		private RecordFormatCompilerHints(CompilerHints parent) {
			copyFrom(parent);
		}

		@Override
		public float getAvgOutputRecordSize() {
			float superWidth = super.getAvgOutputRecordSize();
			if (superWidth > 0.0f || this.width <= 0.0f) {
				return superWidth;
			} else {
				return this.width;
			}
		}

		private void addWidthRecordFormat(float width) {
			this.width += width;
		}
	}
}
