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

import eu.stratosphere.api.record.io.TextInputFormat;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactLong;

public class EdgesWithWeightInputFormat extends TextInputFormat {
	
	private static final long serialVersionUID = 1L;
	
	private final PactLong srcId = new PactLong();
	private final PactLong trgId = new PactLong();
	private final PactLong outLinks = new PactLong();
	
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
		String str = new String(bytes, offset, numBytes);
		String[] parts = str.split("\\s+");

		this.srcId.setValue(Long.parseLong(parts[0]));
		this.trgId.setValue(Long.parseLong(parts[1]));
		this.outLinks.setValue(Long.parseLong(parts[2]));
		
		target.setField(0, this.srcId);
		target.setField(1, this.trgId);
		target.setField(2, this.outLinks);
		return true;
	}

}