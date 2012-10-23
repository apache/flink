/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.example.util;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.FixedLengthInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * InputFormat that reads a binary file and parses the content as integer values.
 * Output records have two fields: first, the parse integer value, second an optional payload
 * string with configurable length.
 * 
 * @author Stephan Ewen
 */
public class BinaryIntInputFormat extends FixedLengthInputFormat
{
	public static final String PAYLOAD_SIZE_PARAMETER_KEY = "int.file.inputformat.payloadSize";
	
	private final PactInteger key = new PactInteger();
	
	private final PactString payLoad = new PactString();
	
	
	
	@Override
	public void configure(Configuration parameters) {
		
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 4);
		super.configure(parameters);
		
		int payLoadSize = (int)parameters.getLong(PAYLOAD_SIZE_PARAMETER_KEY, 0);
		if(payLoadSize > 0) {
			char[] payLoadC = new char[payLoadSize/2];
			for(int i=0;i<payLoadC.length;i++) {
				payLoadC[i] = '.';
			}
			this.payLoad.setValue(new String(payLoadC));
		} else if(payLoadSize == 0) {
			this.payLoad.setValue("");
		} else {
			throw new IllegalArgumentException("PayLoadSize must be >= 0");
		}
	}
	
	@Override
	public boolean readBytes(PactRecord target, byte[] readBuffer, int pos)
	{	
		int key = ((readBuffer[pos] & 0xFF) << 24) |
				  ((readBuffer[pos+1] & 0xFF) << 16) |
				  ((readBuffer[pos+2] & 0xFF) << 8) |
				   (readBuffer[pos+3] & 0xFF);
		
		this.key.setValue(key);
		target.setField(0, this.key);
		target.setField(1, this.payLoad);
		return true;
	}
}
