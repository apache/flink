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


package org.apache.flink.types.parser;

import org.apache.flink.types.DoubleValue;

/**
 * Parses a text field into a DoubleValue.
 */
public class DoubleValueParser extends FieldParser<DoubleValue> {
	
	private DoubleValue result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delim, DoubleValue reusable) {
		
		int i = startPos;
		final byte delByte = (byte) delim;
		
		while (i < limit && bytes[i] != delByte) {
			i++;
		}
		
		String str = new String(bytes, startPos, i-startPos);
		try {
			double value = Double.parseDouble(str);
			reusable.setValue(value);
			this.result = reusable;
			return (i == limit) ? limit : i+1;
		}
		catch (NumberFormatException e) {
			setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
			return -1;
		}
	}
	
	@Override
	public DoubleValue createValue() {
		return new DoubleValue();
	}

	@Override
	public DoubleValue getLastResult() {
		return this.result;
	}
}
