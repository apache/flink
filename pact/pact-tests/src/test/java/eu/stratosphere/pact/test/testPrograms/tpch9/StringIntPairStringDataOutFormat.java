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


package eu.stratosphere.pact.test.testPrograms.tpch9;

import java.io.IOException;

import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class StringIntPairStringDataOutFormat extends FileOutputFormat {

	private final StringBuilder buffer = new StringBuilder();
	private final StringIntPair key = new StringIntPair();
	private final PactString value = new PactString();
	
	@Override
	public void writeRecord(PactRecord record) throws IOException {
		record.getField(0, key);
		record.getField(1, value);
		
		this.buffer.setLength(0);
		this.buffer.append(key.getFirst().toString());
		this.buffer.append('|');
		this.buffer.append(key.getSecond().toString());
		this.buffer.append('|');
		this.buffer.append(value.toString());
		this.buffer.append('\n');
		
		byte[] bytes = this.buffer.toString().getBytes();
		
		this.stream.write(bytes);
	}

}
