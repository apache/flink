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

package eu.stratosphere.nephele.io.channels.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

/**
 */
public class LongType implements SerializationTestType
{
	private long value;
	

	public LongType()
	{
		this.value = 0;
	}
	
	private LongType(long value)
	{
		this.value = value;
	}
	

	@Override
	public LongType getRandom(Random rnd)
	{
		return new LongType(rnd.nextLong());
	}
	

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.value);
	}


	@Override
	public void read(DataInput in) throws IOException
	{
		this.value = in.readLong();
	}


	@Override
	public int hashCode()
	{
		return (int) (this.value ^ this.value >>> 32);
	}


	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof LongType) {
			LongType other = (LongType) obj;
			return this.value == other.value;
		} else {
			return false;
		}
	}
}
