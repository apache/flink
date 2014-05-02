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

package eu.stratosphere.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

/**
 * Boxed serializable and comparable double precision floating point type, representing the primitive
 * type {@code double}.
 * 
 * @see eu.stratosphere.types.Key
 */
public class DoubleValue implements Key, ResettableValue<DoubleValue>, CopyableValue<DoubleValue> {
	private static final long serialVersionUID = 1L;

	private double value;

	/**
	 * Initializes the encapsulated double with 0.0.
	 */
	public DoubleValue() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated double with the provided value.
	 * 
	 * @param value
	 *        Initial value of the encapsulated double.
	 */
	public DoubleValue(double value) {
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
	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public void setValue(DoubleValue value) {
		this.value = value.value;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInput in) throws IOException {
		this.value = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.value);
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}
	
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof DoubleValue)) {
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to DoubleValue!");
		}

		final double other = ((DoubleValue) o).value;

		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	@Override
	public int hashCode() {
		long temp = Double.doubleToLongBits(this.value);
		return 31 + (int) (temp ^ temp >>> 32);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj.getClass() == DoubleValue.class) {
			final DoubleValue other = (DoubleValue) obj;
			return Double.doubleToLongBits(this.value) == Double.doubleToLongBits(other.value);
		}
		return false;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getBinaryLength() {
		return 8;
	}
	
	@Override
	public void copyTo(DoubleValue target) {
		target.value = this.value;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 8);
	}
}
