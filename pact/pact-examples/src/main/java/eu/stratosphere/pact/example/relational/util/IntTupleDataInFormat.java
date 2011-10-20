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

import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class IntTupleDataInFormat extends DelimitedInputFormat {

	public static final int MAX_COLUMNS = 20;

	public static final int DELIMITER = '|';

	@Override
	public boolean readRecord(PactRecord target, byte[] line, int numBytes) {
		int readPos = 0;

		// allocate the offsets array
		short[] offsets = new short[MAX_COLUMNS];

		int col = 1; // the column we are in
		int countInWrapBuffer = 0; // the number of characters in the wrapping buffer

		int startPos = readPos;

		while (readPos < numBytes) {
			if (line[readPos++] == DELIMITER) {
				offsets[col++] = (short) (countInWrapBuffer + readPos - startPos);
			}
		}

		Tuple value = new Tuple(line, offsets, col - 1);
		PactInteger key = new PactInteger((int) value.getLongValueAt(0));
		
		target.setField(0, key);
		target.setField(1, value);

		return true;
	}
}
