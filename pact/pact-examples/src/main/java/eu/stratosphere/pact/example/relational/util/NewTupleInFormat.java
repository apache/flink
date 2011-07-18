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

import eu.stratosphere.pact.common.recordio.DelimitedInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

public class NewTupleInFormat extends DelimitedInputFormat
{

	public static final int MAX_COLUMNS = 20;

	public static final char DELIMITER = '|';

	
	private final Tuple theTuple = new Tuple();
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[])
	 */
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int numBytes)
	{
		this.theTuple.setContents(bytes, 0, numBytes, DELIMITER);
		target.setField(0, this.theTuple);
		return true;
	}
}
