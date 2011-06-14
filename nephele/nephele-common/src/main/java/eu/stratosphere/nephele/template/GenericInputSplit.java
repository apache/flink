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

package eu.stratosphere.nephele.template;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A generic input split that has only a partition number.
 */
public class GenericInputSplit implements InputSplit
{
	protected int partitionNumber;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Default constructor for instantiation during de-serialization.
	 */
	public GenericInputSplit()
	{}
	
	/**
	 * Creates a generic input split with the given partition number.
	 * 
	 * @param partitionNumber The partition number of the split.
	 */
	public GenericInputSplit(int partitionNumber)
	{
		this.partitionNumber = partitionNumber;
	}
	
	
	// --------------------------------------------------------------------------------------------	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.partitionNumber);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		this.partitionNumber = in.readInt();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.InputSplit#getPartitionNumber()
	 */
	@Override
	public int getPartitionNumber()
	{
		return this.partitionNumber;
	}

}
