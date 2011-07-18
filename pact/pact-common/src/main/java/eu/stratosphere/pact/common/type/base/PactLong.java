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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

/**
 * Long base type for PACT programs that implements the Key interface.
 * PactLong encapsulates a Java primitive long.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class PactLong implements Key {

	private long value;

	/**
	 * Initializes the encapsulated long with 0.
	 */
	public PactLong() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated long with the specified value. 
	 * 
	 * @param value Initial value of the encapsulated long.
	 */
	public PactLong(final long value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated long.
	 * 
	 * @return The value of the encapsulated long.
	 */
	public long getValue() {
		return this.value;
	}

	/**
	 * Sets the value of the encapsulated long to the specified value.
	 * 
	 * @param value
	 *        The new value of the encapsulated long.
	 */
	public void setValue(final long value) {
		this.value = value;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.value = in.readLong();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeLong(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactLong))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Integer!");

		final long other = ((PactLong) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return 43 + (int) (this.value ^ this.value >>> 32);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj)
	{
		if (obj != null & obj instanceof PactLong) {
			return this.value == ((PactLong) obj).value;  
		}
		return false;
	}
}
