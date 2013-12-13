/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.io.channels.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

/**
 * @author Stephan Ewen
 */
public class UTFStringType implements SerializationTestType
{
	private static final int MAX_LEN = 1500;
	
	private String value;
	

	public UTFStringType()
	{
		this.value = "";
	}
	
	private UTFStringType(String value)
	{
		this.value = value;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.serialization.SerializationTestType#getRandom(java.util.Random)
	 */
	@Override
	public UTFStringType getRandom(Random rnd)
	{
		final StringBuilder bld = new StringBuilder();
		final int len = rnd.nextInt(MAX_LEN + 1);
		
		for (int i = 0; i < len; i++) {
			bld.append((char) rnd.nextInt(Character.MAX_VALUE));
		}
		
		return new UTFStringType(bld.toString());
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(this.value);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		this.value = in.readUTF();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.value.hashCode();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof UTFStringType) {
			UTFStringType other = (UTFStringType) obj;
			return this.value.equals(other.value);
		} else {
			return false;
		}
	}
}
