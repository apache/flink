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
import java.util.Arrays;
import java.util.Random;

/**
 * @author Stephan Ewen
 */
public class ByteArrayType implements SerializationTestType
{
	private static final int MAX_LEN = 512 * 15;
	
	private byte[] data;
	

	public ByteArrayType()
	{
		this.data = new byte[0];
	}
	
	private ByteArrayType(byte[] data)
	{
		this.data = data;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.serialization.SerializationTestType#getRandom(java.util.Random)
	 */
	@Override
	public ByteArrayType getRandom(Random rnd)
	{
		final int len = rnd.nextInt(MAX_LEN) + 1;
		final byte[] data = new byte[len];
		rnd.nextBytes(data);
		return new ByteArrayType(data);
	}
	
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.data.length);
		out.write(this.data);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		final int len = in.readInt();
		this.data = new byte[len];
		in.readFully(this.data);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return Arrays.hashCode(this.data);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof ByteArrayType) {
			ByteArrayType other = (ByteArrayType) obj;
			return Arrays.equals(this.data, other.data);
		} else {
			return false;
		}
	}
}
