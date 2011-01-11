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
 * Double base type for PACT programs that implements the Key interface.
 * PactDouble encapsulates a Java primitive double.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class PactDouble implements Key {
	
	private double value;

	/**
	 * Initializes the encapsulated int with 0.0.
	 */
	public PactDouble() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated int with the provided value.
	 * 
	 * @param value Initial value of the encapsulated int.
	 */
	public PactDouble(final double value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated primitive double.
	 * 
	 * @return the value of the encapsulated primitive double.
	 */
	public double getValue() {
		return this.value;
	}
	
	/**
	 * Sets the value of the encapsulated primitive double.
	 * 
	 * @param value
	 *        the new value of the encapsulated primitive double.
	 */
	public void setValue(final double value) {
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
		this.value = in.readDouble();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeDouble(this.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof PactDouble))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Double!");

		final double other = ((PactDouble) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(this.value);
		result = prime * result + (int) (temp ^ temp >>> 32);
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
		final PactDouble other = (PactDouble) obj;
		if (Double.doubleToLongBits(this.value) != Double.doubleToLongBits(other.value))
			return false;
		return true;
	}

}
