/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.java.tuple;

import java.io.ObjectStreamException;

/**
 * A tuple with 0 fields.
 * <p>
 * {@code Tuple0} is a singleton.
 * 
 * @see Tuple
 */
public class Tuple0 extends Tuple {
	private static final long serialVersionUID = 1L;

	public static final Tuple0 instance = new Tuple0();

	@Override
	public int getArity() {
		return 0;
	}

	@Override
	public <T> T getField(int pos) {
		throw new IndexOutOfBoundsException(String.valueOf(pos));
	}

	@Override
	public <T> void setField(T value, int pos) {
		throw new IndexOutOfBoundsException(String.valueOf(pos));
	}

	// -------------------------------------------------------------------------------------------------
	// standard utilities
	// -------------------------------------------------------------------------------------------------

	/**
	 * Creates a string representation of the tuple in the form "()"
	 * 
	 * @return The string representation of the tuple.
	 */
	@Override
	public String toString() {
		return "()";
	}

	/**
	 * Deep equality for tuples by calling equals() on the tuple members
	 * 
	 * @param o
	 *            the object checked for equality
	 * @return true if this is equal to o.
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Tuple0)) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	// singleton deserialization
	private Object readResolve() throws ObjectStreamException {
		return instance;
	}

}
