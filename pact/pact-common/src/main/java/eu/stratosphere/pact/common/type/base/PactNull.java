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
import eu.stratosphere.pact.common.type.NormalizableKey;

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
public final class PactNull implements Key, NormalizableKey
{	
	/**
	 * The PactNull singleton instance.
	 */
	private final static PactNull INSTANCE = new PactNull();

	/**
	 * Returns PactNull's singleton instance.
	 *  
	 * @return PactNull's singleton instance.
	 */
	public static PactNull getInstance() {
		return INSTANCE;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a PactNull object.
	 */
	public PactNull() {
	}

	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.types.Record#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		in.readBoolean();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.types.Record#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(false);
	}

	// --------------------------------------------------------------------------------------------
	
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
			throw new ClassCastException("Cannot compare " + o.getClass().getName() + " to PactNull!");
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {
		return (o != null && o.getClass() == PactNull.class);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 53;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.NormalizableKey#getNormalizedKeyLen()
	 */
	@Override
	public int getMaxNormalizedKeyLen()
	{
		return 0;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.NormalizableKey#copyNormalizedKey(byte[], int, int)
	 */
	@Override
	public void copyNormalizedKey(byte[] target, int offset, int len) {
		for (int i = offset; i < offset + len; i++) {
			target[i] = 0;
		}
	}
}
