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

package eu.stratosphere.test.operators.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.api.java.record.io.FileOutputFormat;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class ContractITCaseIOFormats {

	private static final Log LOG = LogFactory.getLog(ContractITCaseIOFormats.class);
	
	public static class ContractITCaseInputFormat extends DelimitedInputFormat {
		private static final long serialVersionUID = 1L;

		private final StringValue keyString = new StringValue();
		private final StringValue valueString = new StringValue();
		
		@Override
		public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {
			this.keyString.setValueAscii(bytes, offset, 1);
			this.valueString.setValueAscii(bytes, offset + 2, 1);
			target.setField(0, keyString);
			target.setField(1, valueString);
			
			if (LOG.isDebugEnabled())
				LOG.debug("Read in: [" + keyString.getValue() + "," + valueString.getValue() + "]");
			
			return target;
		}
	}

	public static class ContractITCaseOutputFormat extends FileOutputFormat {
		private static final long serialVersionUID = 1L;
		
		private final StringBuilder buffer = new StringBuilder();
		private final StringValue keyString = new StringValue();
		private final IntValue valueInteger = new IntValue();
		
		
		public ContractITCaseOutputFormat() {
			setWriteMode(WriteMode.OVERWRITE);
		}
		
		@Override
		public void writeRecord(Record record) throws IOException {
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
