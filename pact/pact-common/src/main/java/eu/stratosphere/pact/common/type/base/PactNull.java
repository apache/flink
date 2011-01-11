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
 * Null base type for PACT programs that implements the Key interface.
 * This type can be used if no key or value is required and serializes into a zero-length byte stream.
 * So no data is shipped or stored using this type.
 * 
 * @see eu.stratosphere.pact.common.type.Key
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public final class PactNull implements Key {
	
	// Singleton PactNull instance
	public final static PactNull INSTANCE = new PactNull();

	/**
	 * Returns PactNull's singleton instance.
	 *  
	 * @return PactNull's singleton instance.
	 */
	public static PactNull getInstance() {
		return INSTANCE;
	}

	/**
	 * Creates a PactNull object.
	 */
	public PactNull() {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		// do nothing
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// do nothing
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "(null)";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Key o) {
		if (o.getClass() != PactNull.class) {
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to N_Null!");
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {
		return (o.getClass() == PactNull.class);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 53;
	}
}
