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
import eu.stratosphere.core.memory.MemorySegment;

/**
 * Null base type for PACT programs that implements the Key interface.
 * This type can be used if no key or value is required and serializes into a zero-length byte stream.
 * So no data is shipped or stored using this type.
 * 
 * @see eu.stratosphere.types.Key
 */
public final class NullValue implements Key, NormalizableKey, CopyableValue<NullValue> {
	private static final long serialVersionUID = 1L;
	
	/**
	 * The PactNull singleton instance.
	 */
	private final static NullValue INSTANCE = new NullValue();

	/**
	 * Returns PactNull's singleton instance.
	 *  
	 * @return PactNull's singleton instance.
	 */
	public static NullValue getInstance() {
		return INSTANCE;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a PactNull object.
	 */
	public NullValue() {
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
	
	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		in.readBoolean();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(false);
	}
	
	// --------------------------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Key o) {
		if (o.getClass() != NullValue.class) {
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
		return (o != null && o.getClass() == NullValue.class);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 53;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int getMaxNormalizedKeyLen() {
		return 0;
	}

	@Override
	public void copyNormalizedKey(MemorySegment target, int offset, int len) {
		for (int i = offset; i < offset + len; i++) {
			target.put(i, (byte) 0);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int getBinaryLength() {
		return 1;
	}
	
	@Override
	public void copyTo(NullValue target) {
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		source.readBoolean();
		target.writeBoolean(false);
	}
}
