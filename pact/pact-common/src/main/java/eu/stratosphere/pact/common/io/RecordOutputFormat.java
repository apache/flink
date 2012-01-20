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


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.parser.FieldParser;


/**
 * This is an Outputformat to serialize {@link PactRecord}s into ASCII text. 
 * The output is structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common).
 * Field delimiters separate fields within a record. 
 * Record and field delimiters can be configured using the RecordOutputFormat {@link Configuration}.
 * 
 * The number of fields to serialize must be configured as well.  
 * For each field the type of the {@link Value} must be specified using the {@link RecordOutputFormat#FIELD_TYPE_PARAMETER_PREFIX} config key
 * and an index running from 0 to the number of fields.
 *  
 * The position within the {@link PactRecord} can be configured for each field using the 
 * {@link RecordOutputFormat#RECORD_POSITION_PARAMETER_PREFIX} config key.
 * Either all {@link PactRecord} postions must be configured or none. If none is configured, the index of the config key is used.
 * 
 * @see Value
 * @see Configuration
 * @see PactRecord
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class RecordOutputFormat extends FileOutputFormat
{
	public static final String RECORD_DELIMITER_PARAMETER = "outputformat.delimiter.record";
	
	public static final String FIELD_DELIMITER_PARAMETER = "outputformat.delimiter.field";
	
	public static final String NUM_FIELDS_PARAMETER = "outputformat.field.number";
	
	public static final String FIELD_TYPE_PARAMETER_PREFIX = "outputformat.field.type_";
	
	public static final String RECORD_POSITION_PARAMETER_PREFIX = "outputformat.record.position_";
	
	public static final String LENIENT_PARSING = "outputformat.lenient.parsing";
	
	private static final Log LOG = LogFactory.getLog(RecordOutputFormat.class);
	
	// --------------------------------------------------------------------------------------------
	
	private int numFields;
	
	private Class<? extends Value>[] classes;
	
	private int[] recordPositions;

	private Writer wrt;
	
	private String fieldDelimiter;
	
	private String recordDelimiter;
	
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
			throw new IllegalArgumentException("Invalid configuration for RecordOutputFormat: " +
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
				throw new IllegalArgumentException("Invalid configuration for RecordOutputFormat: " +
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
					throw new IllegalArgumentException("Invalid configuration for RecordOutputFormat: " +
							"Invalid record position for parameter " + i);
				}

				this.recordPositions[i] = pos;
				
			} else {
				allRecordPosDefined = false;
				
				this.recordPositions[i] = i;
			}
		}
		
		if(anyRecordPosDefined && !allRecordPosDefined) {
			throw new IllegalArgumentException("Invalid configuration for RecordOutputFormat: " +
					"Either none or all record positions must be defined.");
		}
		
		this.recordDelimiter = parameters.getString(RECORD_DELIMITER_PARAMETER, "\n");
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
		this.wrt = new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096));
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

}
