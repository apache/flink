/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.test.contracts.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class ContractITCaseIOFormats {

	private static final Log LOG = LogFactory.getLog(ContractITCaseIOFormats.class);
	
	public static class ContractITCaseInputFormat extends DelimitedInputFormat {

		private final PactString keyString = new PactString();
		private final PactString valueString = new PactString();
		
		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
			this.keyString.setValueAscii(bytes, offset, 1);
			this.valueString.setValueAscii(bytes, offset + 2, 1);
			target.setField(0, keyString);
			target.setField(1, valueString);
			
			if (LOG.isDebugEnabled())
				LOG.debug("Read in: [" + keyString.getValue() + "," + valueString.getValue() + "]");
			
			return true;
		}

	}

	public static class ContractITCaseOutputFormat extends FileOutputFormat
	{
		private final StringBuilder buffer = new StringBuilder();
		private final PactString keyString = new PactString();
		private final PactInteger valueInteger = new PactInteger();
		
		@Override
		public void writeRecord(PactRecord record) throws IOException {
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, keyString).toString());
			this.buffer.append(' ');
			this.buffer.append(record.getField(1, valueInteger).getValue());
			this.buffer.append('\n');
			
			byte[] bytes = this.buffer.toString().getBytes();
			
			if (LOG.isDebugEnabled())
				LOG.debug("Writing out: [" + keyString.toString() + "," + valueInteger.getValue() + "]");
			
			this.stream.write(bytes);
		}
	}
}
