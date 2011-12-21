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

package eu.stratosphere.pact.example.relational.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Dedicated input format for the {@link Tuple} data type. This input format returns
 * records with a single field, containing a <tt>Tuple</tt> with the contents of
 * the byte string.
 * <p>
 * The delimiter for the tuple fields can be configured using the parameters with the
 * parameter key {@link TupleInFormat#} 
 *
 * @author Stephan Ewen
 */
public class TupleInFormat extends DelimitedInputFormat
{
	/**
	 * The config key to define the delimiter character that separates fields in the tuples.
	 */
	public static final String TUPLE_FIELD_DELIMITER = "tupleformat.delimiter";
	
	/**
	 * The default delimiter character that separates fields in the tuples.
	 */
	public static final char DEFAULT_DELIMITER = '|';
	
	// --------------------------------------------------------------------------------------------
	
	protected static final Log LOG = LogFactory.getLog(TupleInFormat.class);
	
	protected final Tuple theTuple = new Tuple();			// the reused instance of the tuple
	
	protected char delimiter;							// the delimiter as read from the configuration

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Nullary constructor for serialization.
	 */
	public TupleInFormat()
	{}
	
	/**
	 * Package private constructor for testing.
	 * 
	 * @param delimiter The delimiter to use.
	 */
	TupleInFormat(char delimiter)
	{
		this.delimiter = delimiter;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{
		super.configure(parameters);
		
		final int del = parameters.getInteger(TUPLE_FIELD_DELIMITER, DEFAULT_DELIMITER);
		if (del < 0 || del > Character.MAX_VALUE) {
			throw new RuntimeException("Invalid delimiter - exceeds character value range: " + del);
		}
		this.delimiter = (char) del;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[])
	 */
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int numBytes)
	{
		try {
			this.theTuple.setContents(bytes, 0, numBytes, this.delimiter);
			target.setField(0, this.theTuple);
			return true;
		}
		catch (RuntimeException rex) {
			if (LOG.isErrorEnabled())
				LOG.error("Could not parse byte record to tuple: " + (new String(bytes, 0, numBytes)), rex);
			return false;
		}
	}
}
