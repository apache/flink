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

package eu.stratosphere.spargel.java.examples.connectedcomponents;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactRecord;

import java.util.regex.Pattern;

public class LongLongInputFormat extends TextInputFormat {

	private static final long serialVersionUID = 1L;

	private static final Pattern SEPARATOR = Pattern.compile("[,\t ]");
	
	private final PactLong l1 = new PactLong();
	private final PactLong l2 = new PactLong();

	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
		String str = new String(bytes, offset, numBytes);
		String[] parts = SEPARATOR.split(str);

		this.l1.setValue(Long.parseLong(parts[0]));
		this.l2.setValue(Long.parseLong(parts[1]));
		
		target.setField(0, this.l1);
		target.setField(1, this.l2);
		return true;
	}
}
