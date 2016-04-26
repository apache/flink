/*
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


package org.apache.flink.types;

import java.io.IOException;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

/**
 * Null base type for programs that implements the Key interface.
 * 
 * @see org.apache.flink.types.Key
 */
@Public
public final class NullValue implements NormalizableKey<NullValue>, CopyableValue<NullValue> {
	private static final long serialVersionUID = 1L;
	
	/**
	 * The singleton NullValue instance.
	 */
	private final static NullValue INSTANCE = new NullValue();

	/**
	 * Returns the NullValue singleton instance.
	 *  
	 * @return The NullValue singleton instance.
	 */
	public static NullValue getInstance() {
		return INSTANCE;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a NullValue object.
	 */
	public NullValue() {}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return "(null)";
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		in.readBoolean();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeBoolean(false);
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int compareTo(NullValue o) {
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		return (o != null && o.getClass() == NullValue.class);
	}

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
	public NullValue copy() {
		return NullValue.getInstance();
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		source.readBoolean();
		target.writeBoolean(false);
	}
}
