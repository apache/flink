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

package eu.stratosphere.pact.common.type.base.parser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactLong;

/**
 * Parses a decimal text field into a PactLong.
 * Only characters '1' to '0' and '-' are allowed.
 * The parser does not check for the overflows and underflows.
 */
public class DecimalTextLongParser  implements FieldParser<PactLong> {

	@Override
	public void configure(Configuration config) { }
	
	@Override
	public int parseField(byte[] bytes, int startPos, int length, char delim, PactLong field) {
		
		long val = 0;
		boolean neg = false;
		
		if(bytes[startPos] == '-') {
			neg = true;
			startPos++;
		}
		
		for(int i=startPos; i < length; i++) {
			if(bytes[i] == delim) {
				field.setValue(val*(neg ? -1 : 1));
				return i+1;
			}
			if(bytes[i] < 48 || bytes[i] > 57) {
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;
		}
		field.setValue(val*(neg ? -1 : 1));
		return length;
	}
	
	@Override
	public PactLong getValue() {
		return new PactLong();
	}
	
	public static final long parseField(byte[] bytes, int startPos, int length, char delim) {
		if (length <= 0) {
			throw new NumberFormatException("Invalid input: Empty string");
		}
		long val = 0;
		boolean neg = false;
		
		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;
			length--;
			if (length == 0) {
				throw new NumberFormatException("Orphaned minus sign.");
			}
		}
		
		for (; length > 0; startPos++, length--) {
			if (bytes[startPos] == delim) {
				return neg ? -val : val;
			}
			if (bytes[startPos] < 48 || bytes[startPos] > 57) {
				throw new NumberFormatException();
			}
			val *= 10;
			val += bytes[startPos] - 48;
		}
		return neg ? -val : val;
	}
}
