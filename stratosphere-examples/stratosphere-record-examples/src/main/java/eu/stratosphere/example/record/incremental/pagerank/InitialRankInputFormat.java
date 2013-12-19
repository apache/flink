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

package eu.stratosphere.example.record.incremental.pagerank;

import eu.stratosphere.api.record.io.DelimitedInputFormat;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;

public class InitialRankInputFormat extends DelimitedInputFormat {
	
	private static final long serialVersionUID = 1L;
	
	private final LongValue vId = new LongValue();
	private final DoubleValue initialRank = new DoubleValue();
	
	@Override
	public boolean readRecord(Record target, byte[] bytes, int offset, int numBytes) {
		String str = new String(bytes, offset, numBytes);
		String[] parts = str.split("\\s+");

		this.vId.setValue(Long.parseLong(parts[0]));
		this.initialRank.setValue(Double.parseDouble(parts[1]));
		
		target.setField(0, this.vId);
		target.setField(1, this.initialRank);
		return true;
	}

}