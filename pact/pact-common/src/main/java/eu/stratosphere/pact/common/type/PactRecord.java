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
 * The Pact Record is the basic data record that flows between functions in a Pact program. The record is a tuple of
 * arbitrary values.
 * 
 * <p>
 * The Pact Record implements a sparse tuple model, meaning that the record can contain many fields which are actually
 * null and not represented in the record. It has internally a bitmap marking which fields are set and which are not. 
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class PactRecord implements IOReadableWritable
{
	private byte[] binaryData;
	
	private int binaryLen;
	
	private int numFields;
	
	private Value[] fields;
	
	private int[] offsets;
	
	private long sparsityMask;
	
	
	/**
	 * Required nullary constructor for instantiation by serialization logic.
	 */
	public PactRecord()
	{}
	
	/**
	 * Creates a new record containing only a single field, which is the given value.
	 * 
	 * @param value The value for the single field of the record.
	 */
	public PactRecord(Value value)
	{
		
	}
	
	/**
	 * Creates a new record containing exactly to fields, which are the given values.
	 * 
	 * @param val1 The value for the first field.
	 * @param val2 The value for the second field.
	 */
	public PactRecord(Value val1, Value val2)
	{
		
	}
	
	/**
	 * Creates a new pact record, containing the given number of fields. The fields are initially all nulls.
	 *  
	 * @param numFields The number of fields for the record.
	 */
	public PactRecord(int numFields)
	{
		
	}

	// --------------------------------------------------------------------------------------------
	//                             Basic Accessors
	// --------------------------------------------------------------------------------------------
	
	public int getNumFields()
	{
		return this.numFields;
	}
	
	public <T extends Value> T getField(int fieldNum, Class<T> type)
	{
		return null;
	}
	
	public void getField(int fieldNum, Value target)
	{
	}
	
	public void setField(int fieldNum, Value value)
	{
	}
	
	public void removeField(int field) {
		
	}
	
	public void project(long mask) {
		
	}
	
	public void project(long[] mask) {
		
	}
	
	public void setNull(int field) {
		
	}
	
	public void setNull(long fields) {
		
	}
	
	public void setNull(long[] fields) {
		
	}
	
	public void clear()
	{
		
	}
	
	public void unionFields(PactRecord other)
	{
		
	}
	
	public void copyTo(PactRecord target)
	{
		
	}
	
	public PactRecord createCopy()
	{
		final PactRecord rec = new PactRecord();
		copyTo(rec);
		return rec;
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
