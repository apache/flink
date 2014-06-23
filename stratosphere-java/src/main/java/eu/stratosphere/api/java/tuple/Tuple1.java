/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM eu.stratosphere.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------


package eu.stratosphere.api.java.tuple;

import eu.stratosphere.util.StringUtils;

/**
 * A tuple with 1 fields. Tuples are strongly typed; each field may be of a separate type.
 * The fields of the tuple can be accessed directly as public fields (f0, f1, ...) or via their position
 * through the {@link #getField(int)} method. The tuple field positions start at zero.
 * <p>
 * Tuples are mutable types, meaning that their fields can be re-assigned. This allows functions that work
 * with Tuples to reuse objects in order to reduce pressure on the garbage collector.
 *
 * @see Tuple
 *
 * @param <T0> The type of field 0
 */
public class Tuple1<T0> extends Tuple {

	private static final long serialVersionUID = 1L;

	/** Field 0 of the tuple. */
	public T0 f0;

	/**
	 * Creates a new tuple where all fields are null.
	 */
	public Tuple1() {}

	/**
	 * Creates a new tuple and assigns the given values to the tuple's fields.
	 *
	 * @param value0 The value for field 0
	 */
	public Tuple1(T0 value0) {
		this.f0 = value0;
	}

	@Override
	public int getArity() { return 1; }

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getField(int pos) {
		switch(pos) {
			case 0: return (T) this.f0;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> void setField(T value, int pos) {
		switch(pos) {
			case 0:
				this.f0 = (T0) value;
				break;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	/**
	 * Sets new values to all fields of the tuple.
	 *
	 * @param value0 The value for field 0
	 */
	public void setFields(T0 value0) {
		this.f0 = value0;
	}


	// -------------------------------------------------------------------------------------------------
	// standard utilities
	// -------------------------------------------------------------------------------------------------

	/**
	 * Creates a string representation of the tuple in the form
	 * (f0),
	 * where the individual fields are the value returned by calling {@link Object#toString} on that field.
	 * @return The string representation of the tuple.
	 */
	@Override
	public String toString() {
		return "(" + StringUtils.arrayAwareToString(this.f0)
			+ ")";
	}

	/**
	 * Deep equality for tuples by calling equals() on the tuple members
	 * @param o the object checked for equality
	 * @return true if this is equal to o.
	 */
	@Override
	public boolean equals(Object o) {
		if(this == o) { return true; }
		if (!(o instanceof Tuple1)) { return false; }
		Tuple1 tuple = (Tuple1) o;
		if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) { return false; }
		return true;
	}

	@Override
	public int hashCode() {
		int result = f0 != null ? f0.hashCode() : 0;
		return result;
	}
	/**
	* Shallow tuple copy.
	* @returns A new Tuple with the same fields as this.
	 */
	public Tuple1<T0> copy(){ 
		return new Tuple1<T0>(this.f0);
	}

}
