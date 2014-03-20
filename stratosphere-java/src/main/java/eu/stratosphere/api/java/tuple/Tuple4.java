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

@SuppressWarnings({"restriction"})
public class Tuple4<T0, T1, T2, T3> extends Tuple {

	private static final long serialVersionUID = 1L;

	public T0 f0;
	public T1 f1;
	public T2 f2;
	public T3 f3;

	public Tuple4() {}

	public Tuple4(T0 value0, T1 value1, T2 value2, T3 value3) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
		this.f3 = value3;
	}

	@Override
	public int getArity() { return 4; }

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getField(int pos) {
		switch(pos) {
			case 0: return (T) this.f0;
			case 1: return (T) this.f1;
			case 2: return (T) this.f2;
			case 3: return (T) this.f3;
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
			case 1:
				this.f1 = (T1) value;
				break;
			case 2:
				this.f2 = (T2) value;
				break;
			case 3:
				this.f3 = (T3) value;
				break;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}
	public void setFields(T0 value0, T1 value1, T2 value2, T3 value3) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
		this.f3 = value3;
	}


	// -------------------------------------------------------------------------------------------------
	// standard utilities
	// -------------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return "(" + StringUtils.arrayAwareToString(this.f0)
			+ ", " + StringUtils.arrayAwareToString(this.f1)
			+ ", " + StringUtils.arrayAwareToString(this.f2)
			+ ", " + StringUtils.arrayAwareToString(this.f3)
			+ ")";
	}

	// -------------------------------------------------------------------------------------------------
	// unsafe fast field access
	// -------------------------------------------------------------------------------------------------

	@SuppressWarnings({ "unchecked"})
	public <T> T getFieldFast(int pos) {
		return (T) UNSAFE.getObject(this, offsets[pos]);
	}

	private static final sun.misc.Unsafe UNSAFE = eu.stratosphere.core.memory.MemoryUtils.UNSAFE;

	private static final long[] offsets = new long[4];

	static {
		try {
			offsets[0] = UNSAFE.objectFieldOffset(Tuple4.class.getDeclaredField("f0"));
			offsets[1] = UNSAFE.objectFieldOffset(Tuple4.class.getDeclaredField("f1"));
			offsets[2] = UNSAFE.objectFieldOffset(Tuple4.class.getDeclaredField("f2"));
			offsets[3] = UNSAFE.objectFieldOffset(Tuple4.class.getDeclaredField("f3"));
		} catch (Throwable t) {
			throw new RuntimeException("Could not initialize fast field accesses for tuple data type.");
		}
	}
}
