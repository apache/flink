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

package eu.stratosphere.pact.common.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;


/**
 *
 *
 * @author Stephan Ewen
 */
public class PactRecord implements IOReadableWritable
{
	private byte[] binaryData;
	
	private int binaryLen;
	
	private int numFields;
	
	private Value[] fields;
	
	private int offsets[];
	
	private long sparsityMask;
	
	private Class<? extends Value>[] fieldTypes;
	
	
	/**
	 * Required nullary constructor for instantiation by serialization logic.
	 */
	public PactRecord() {
	}
	
	// --------------------------------------------------------------------------------------------
	//                             Serialization
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		// write the length first, variably encoded
		int len = this.binaryLen;
		while (len >= MAX_BIT) {
			out.write(len | MAX_BIT);
			len >>= 7;
		}
		out.write(len);
		
		// 
		out.write(this.binaryData, 0, len);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		// read length, variably encoded
		int len = in.readUnsignedByte();
		if (len >= MAX_BIT) {
			int shift = 7;
			int curr;
			len = len & 0x7f;
			while ((curr = in.readUnsignedByte()) >= MAX_BIT) {
				len |= (curr & 0x7f) << shift;
				shift += 7;
			}
			len |= curr << shift;
		}
		this.binaryLen = len;
		
		// ensure out byte array is large enough
		byte[] data = this.binaryData;
		if (data == null || data.length < len) {
			data = new byte[len];
			this.binaryData = data;
		}
		
		// read the binary data
		in.readFully(this.binaryData, 0, len);
		int offset = 1;
		
		// read number of fields, variable length encoded
		int numFields = data[0];
		if (numFields >= MAX_BIT) {
			int shift = 7;
			int curr;
			numFields = numFields & 0x7f;
			while ((curr = data[offset++]) >= MAX_BIT) {
				numFields |= (numFields & 0x7f) << shift;
				shift += 7;
			}
			numFields |= curr << shift;
		}
		this.numFields = numFields;
		
		
	}
	
	private static final int MAX_BIT = 0x1 << 7;
}
