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
 * @author Stephan Ewen
 */
public class FloatType implements SerializationTestType
{
	private float value;
	

	public FloatType()
	{
		this.value = 0;
	}
	
	private FloatType(float value)
	{
		this.value = value;
	}
	

	@Override
	public FloatType getRandom(Random rnd)
	{
		return new FloatType(rnd.nextFloat());
	}
	

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeFloat(this.value);
	}


	@Override
	public void read(DataInput in) throws IOException
	{
		this.value = in.readFloat();
	}


	@Override
	public int hashCode()
	{
		return Float.floatToIntBits(this.value);
	}


	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof FloatType) {
			FloatType other = (FloatType) obj;
			return Float.floatToIntBits(this.value) == Float.floatToIntBits(other.value);
		} else {
			return false;
		}
	}
}
