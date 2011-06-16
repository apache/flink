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

package eu.stratosphere.pact.common.recordio;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;


/**
 * 
 */
public class TupleOutputFormat extends FileOutputFormat
{
	public static final String NUM_FIELDS_PARAMETER = "outputformat.field.number";
	
	public static final String FIELD_TYPE_PARAMETER_PREFIX = "outputformat.field.type_";
	
	private static final Log LOG = LogFactory.getLog(TupleOutputFormat.class);
	
	
	// --------------------------------------------------------------------------------------------
	
	private Class<? extends Value>[] classes;

	private boolean lenient;
	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Configuration parameters)
	{
		int numFields = parameters.getInteger(NUM_FIELDS_PARAMETER, -1);
		if (numFields < 1) {
			throw new RuntimeException("Invalid configuration for DelimitedOutputFormat: " +
					"Need to specify number of fields > 0.");
		}
		
		@SuppressWarnings("unchecked")
		Class<Value>[] arr = new Class[numFields];
		this.classes = arr;
		
		for (int i = 0; i < numFields; i++)
		{
			@SuppressWarnings("unchecked")
			Class<? extends Value> clazz = (Class<? extends Value>) parameters.getClass(FIELD_TYPE_PARAMETER_PREFIX + i, null);
			if (clazz == null) {
				throw new RuntimeException("Invalid configuration for DelimitedOutputFormat: " +
					"No type class for parameter " + i);
			}
			
			this.classes[i] = clazz;
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.OutputFormat#writeRecord(eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public void writeRecord(PactRecord record) throws IOException
	{
		int numFields = record.getNumFields();
		
		if (numFields > this.classes.length) {
			if (this.lenient) {
				numFields = this.classes.length;
				if (LOG.isWarnEnabled()) 
					LOG.warn("Serializing only first " + numFields + " fields from record.");
			}
			else {
				throw new RuntimeException(
					"Cannot serialize record with more fields than the DelimitedOutputFormat knows types.");
			}			
		}
		
		for (int i = 0; i < numFields; i++) {
			record.getField(i, this.classes[i]);
			
			// add the text and the attribute delimiter
		}
		
		// add the record delimiter
	}

}
