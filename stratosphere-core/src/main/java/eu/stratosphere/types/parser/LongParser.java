/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.types.parser;

/**
 * Parses a decimal text field into a LongValue.
 * Only characters '1' to '0' and '-' are allowed.
 */
public class LongParser extends FieldParser<Long> {
	
	private long result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delimiter, Long reusable) {
		long val = 0;
		boolean neg = false;
		
		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;
			
			// check for empty field with only the sign
			if (startPos == limit || bytes[startPos] == delimiter) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_ORPHAN_SIGN);
				return -1;
			}
		}
		
		for (int i = startPos; i < limit; i++) {
			if (bytes[i] == delimiter) {
				this.result = neg ? -val : val;
				return i+1;
			}
			if (bytes[i] < 48 || bytes[i] > 57) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;
			
			// check for overflow / underflow
			if (val < 0) {
				// this is an overflow/underflow, unless we hit exactly the Long.MIN_VALUE
				if (neg && val == Long.MIN_VALUE) {
					this.result = Long.MIN_VALUE;
					
					if (i+1 >= limit) {
						return limit; 
					} else if (bytes[i+1] == delimiter) {
						return i+2;
					} else {
						setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);
						return -1;
					}
				}
				else {
					setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);
					return -1;
				}
			}
		}
		
		this.result = neg ? -val : val;
		return limit;
	}
	
	@Override
	public Long createValue() {
		return Long.MIN_VALUE;
	}
	
	@Override
	public Long getLastResult() {
		return Long.valueOf(this.result);
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
