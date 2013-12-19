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
 * Boxed serializable and comparable single precision floating point type, representing the primitive
 * type {@code float}.
 * 
 * @see eu.stratosphere.types.Key
 */
public class FloatValue implements Key, CopyableValue<FloatValue> {
	private static final long serialVersionUID = 1L;

	private float value;

	/**
	 * Initializes the encapsulated float with 0.0.
	 */
	public FloatValue() {
		this.value = 0;
	}

	/**
	 * Initializes the encapsulated float with the provided value.
	 * 
	 * @param value
	 *        Initial value of the encapsulated float.
	 */
	public FloatValue(float value) {
		this.value = value;
	}

	/**
	 * Returns the value of the encapsulated primitive float.
	 * 
	 * @return the value of the encapsulated primitive float.
	 */
	public float getValue() {
		return this.value;
	}

	/**
	 * Sets the value of the encapsulated primitive float.
	 * 
	 * @param value
	 *        the new value of the encapsulated primitive float.
	 */
	public void setValue(float value) {
		this.value = value;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInput in) throws IOException {
		this.value = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(this.value);
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}
	
	@Override
	public int compareTo(final Key o) {
		if (!(o instanceof FloatValue))
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to FloatValue!");

		final double other = ((FloatValue) o).value;
		return this.value < other ? -1 : this.value > other ? 1 : 0;
	}

	@Override
	public int hashCode() {
		return Float.floatToIntBits(this.value);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj.getClass() == FloatValue.class) {
			final FloatValue other = (FloatValue) obj;
			return Float.floatToIntBits(this.value) == Float.floatToIntBits(other.value);
		}
		return false;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getBinaryLength() {
		return 4;
	}
	
	@Override
	public void copyTo(FloatValue target) {
		target.value = this.value;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 4);
	}
}
