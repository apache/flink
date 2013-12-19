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

package eu.stratosphere.example.record.sort.terasort;

import eu.stratosphere.api.record.io.DelimitedInputFormat;
import eu.stratosphere.types.Record;

/**
 * This class is responsible for converting a line from the input file to a two field record. 
 * Lines which do not match the expected length are skipped.
 */
public final class TeraInputFormat extends DelimitedInputFormat {
	private static final long serialVersionUID = 1L;
	
	private final TeraKey key = new TeraKey();
	private final TeraValue value = new TeraValue();
	

	@Override
	public boolean readRecord(Record target, byte[] record, int offset, int numBytes)
	{
		if (numBytes != (TeraKey.KEY_SIZE + TeraValue.VALUE_SIZE)) {
			return false;
		}

		this.key.setValue(record, offset);
		this.value.setValue(record, offset + TeraKey.KEY_SIZE);
		
		target.setField(0, this.key);
		target.setField(1, this.value);
		return true;
	}
}
