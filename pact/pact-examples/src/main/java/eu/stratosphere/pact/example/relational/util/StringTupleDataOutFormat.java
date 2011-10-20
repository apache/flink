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

import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

public class StringTupleDataOutFormat extends DelimitedOutputFormat {

	@Override
	public int serializeRecord(PactRecord rec, byte[] target) throws Exception {
		Tuple tuple = rec.getField(0, Tuple.class);
		String tupleStr = tuple.toString();
		byte[] tupleBytes = tupleStr.getBytes();
		
		if(target.length >= tupleBytes.length) {
			System.arraycopy(tupleBytes, 0, target, 0, tupleBytes.length);
			return tupleBytes.length;
		} else {
			return -1 * tupleBytes.length;
		}
	}

}
