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

public class PactLong implements Key {

	private long value;

	public PactLong() {
		this.value = 0;
	}

	public PactLong(final long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.value = in.readLong();
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeLong(this.value);
	}

	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactLong))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Integer!");

		final long other = ((PactLong) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	/**
	 * Returns the value.
	 * 
	 * @return the value
	 */
	public long getValue() {
		return this.value;
	}

	/**
	 * Sets the value to the specified value.
	 * 
	 * @param value
	 *        the value to set
	 */
	public void setValue(final long value) {
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 43;
		int result = 1;
		result = prime * result + (int) (this.value ^ this.value >>> 32);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final PactLong other = (PactLong) obj;
		if (this.value != other.value)
			return false;
		return true;
	}

}
