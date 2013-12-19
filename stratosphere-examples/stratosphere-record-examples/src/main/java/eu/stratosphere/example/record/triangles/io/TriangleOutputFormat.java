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

package eu.stratosphere.example.record.triangles.io;

import eu.stratosphere.api.record.io.DelimitedOutputFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;


/**
 * 
 */
public final class TriangleOutputFormat extends DelimitedOutputFormat {
	private static final long serialVersionUID = 1L;
	
	private final StringBuilder line = new StringBuilder();

	/* (non-Javadoc)
	 * @see eu.stratosphere.api.record.io.DelimitedOutputFormat#serializeRecord(eu.stratosphere.pact.common.type.Record, byte[])
	 */
	@Override
	public int serializeRecord(Record rec, byte[] target) throws Exception {
		final int e1 = rec.getField(0, IntValue.class).getValue();
		final int e2 = rec.getField(1, IntValue.class).getValue();
		final int e3 = rec.getField(2, IntValue.class).getValue();
		
		this.line.setLength(0);
		this.line.append(e1);
		this.line.append(',');
		this.line.append(e2);
		this.line.append(',');
		this.line.append(e3);
		
		if (target.length >= line.length()) {
			for (int i = 0; i < line.length(); i++) {
				target[i] = (byte) line.charAt(i);
			}
			return line.length();
		} else {
			return -line.length();
		}
	}
}