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

package eu.stratosphere.pact.common.type.base.parser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Converts a fixed length portion of a byte array into a {@link PactString}.
 * Checks that the field is terminated either by end of array or field delimiter character.
 * A string encapsulator can be configured. If configured, the encapsulator must be present in the input but 
 * will not be included in the PactString.
 * The fixed length must include possible encapsulators.   
 * 
 * @author Fabian Hueske
 * 
 * @see PactString
 *
 */
public class FixedLengthStringParser implements FieldParser<PactString> {

	public static final String STRING_ENCAPSULATOR = "fixlength.string.parser.encapsulator";
	public static final String STRING_LENGTH = "fixlength.string.parser.length";
	
	private char encapsulator;
	private boolean encapsulated;
	private int fixLength;
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	public void configure(Configuration config) {
		fixLength = config.getInteger(STRING_LENGTH, -1);
		if(fixLength < 1) {
			throw new IllegalArgumentException("FixedLengthStringParser: No or invalid string length provided.");
		}
		String encapStr = config.getString(STRING_ENCAPSULATOR, "#*+~#**");
		if(encapStr.equals("#*+~#**")) {
			encapsulated = false;
		} else {
			encapsulated = true;
			if(encapStr.length() != 1) {
				throw new IllegalArgumentException("FixedLengthStringParser: String encapsulator must be exactly one character.");
			}
			encapsulator = encapStr.charAt(0);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#parseField(byte[], int, int, char, eu.stratosphere.pact.common.type.Value)
	 */
	@Override
	public int parseField(byte[] bytes, int startPos, int length, char delim, PactString field) {
	
		if(startPos+fixLength > length) {
			// not enough bytes left
			return -1;
		}
		
		if(startPos+fixLength < length && bytes[startPos+fixLength] != delim) {
			// more bytes left, but no delimiter at next position
			return -1;
		}
		
		if(encapsulated) {
			if(bytes[startPos] != encapsulator || bytes[startPos+fixLength-1] != encapsulator) {
				// string not encapsulated
				return -1;
			}
			field.setValueAscii(bytes, startPos+1, fixLength-2);
		} else {
			field.setValueAscii(bytes, startPos, fixLength);
		}
		return (startPos+fixLength) == length ? length : startPos+fixLength+1;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#getValue()
	 */
	@Override
	public PactString getValue() {
		return new PactString();
	}
	
}