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

package eu.stratosphere.pact.test.testPrograms.util;

import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactString;

public class StringTupleDataOutFormat extends DelimitedOutputFormat {
	private static final long serialVersionUID = 1L;

	@Override
	public int serializeRecord(PactRecord rec, byte[] target) throws Exception {
		String string = rec.getField(0, PactString.class).toString();
		byte[] stringBytes = string.getBytes();
		Tuple tuple = rec.getField(1, Tuple.class);
		String tupleStr = tuple.toString();
		byte[] tupleBytes = tupleStr.getBytes();
		int totalLength = stringBytes.length + 1 + tupleBytes.length;
		if(target.length >= totalLength) {
			System.arraycopy(stringBytes, 0, target, 0, stringBytes.length);
			target[stringBytes.length] = '|';
			System.arraycopy(tupleBytes, 0, target, stringBytes.length + 1, tupleBytes.length);
			return totalLength;
		} else {
			return -1 * totalLength;
		}
	}

}
