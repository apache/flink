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

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactString;

public class StringTupleDataInFormat extends TextInputFormat<PactString, Tuple> {

	private static final Log LOG = LogFactory.getLog(StringTupleDataInFormat.class);
	
	private final int maxColumns = 20;
	private final int delimiter = '|';
	
	@Override
	public boolean readLine(KeyValuePair<PactString, Tuple> pair, byte[] line) {
		int readPos = 0;

		// allocate the offsets array
		short[] offsets = new short[maxColumns];

		int col = 1; // the column we are in
		int countInWrapBuffer = 0; // the number of characters in the wrapping buffer

		int startPos = readPos;

		while (readPos < line.length) {
			if (line[readPos++] == delimiter) {
				offsets[col++] = (short) (countInWrapBuffer + readPos - startPos);
			}
		}

		Tuple value = new Tuple(line, offsets, col - 1);
		PactString key = new PactString(value.getStringValueAt(0));

		pair.setKey(key);
		pair.setValue(value);
		
		LOG.debug("Emit: "+key+" , "+value);

		return true;
	}
}
