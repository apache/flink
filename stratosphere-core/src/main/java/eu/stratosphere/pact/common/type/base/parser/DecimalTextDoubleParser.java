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

import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * Parses a text field into a PactDouble.
 */
public class DecimalTextDoubleParser extends FieldParser<PactDouble> {
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delim, PactDouble field) {
		
		int i = startPos;
		final byte delByte = (byte) delim;
		
		while (i < limit && bytes[i] != delByte) {
			i++;
		}
		
		String str = new String(bytes, startPos, i-startPos);
		try {
			double value = Double.parseDouble(str);
			field.setValue(value);
			return (i == limit) ? limit : i+1;
		}
		catch (NumberFormatException e) {
			return -1;
		}
	}
	
	@Override
	public PactDouble createValue() {
		return new PactDouble();
	}
}
