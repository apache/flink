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
public class Tuple5<T1, T2, T3, T4, T5> extends Tuple {

	private static final long serialVersionUID = 1L;

	private T1 _1;
	private T2 _2;
	private T3 _3;
	private T4 _4;
	private T5 _5;

	public Tuple5() {}

	public Tuple5(T1 value1, T2 value2, T3 value3, T4 value4, T5 value5) {
		this._1 = value1;
		this._2 = value2;
		this._3 = value3;
		this._4 = value4;
		this._5 = value5;
	}

	@Override
	public int getArity() { return 5; }

	public T1 T1() {
		return this._1;
	}
	public T2 T2() {
		return this._2;
	}
	public T3 T3() {
		return this._3;
	}
	public T4 T4() {
		return this._4;
	}
	public T5 T5() {
		return this._5;
	}
	public void T1(T1 value) {
		this._1 = value;
	}
	public void T2(T2 value) {
		this._2 = value;
	}
	public void T3(T3 value) {
		this._3 = value;
	}
	public void T4(T4 value) {
		this._4 = value;
	}
	public void T5(T5 value) {
		this._5 = value;
	}
	@Override
	@SuppressWarnings("unchecked")
	public <T> T getField(int pos) {
		switch(pos) {
			case 0: return (T) this._1;
			case 1: return (T) this._2;
			case 2: return (T) this._3;
			case 3: return (T) this._4;
			case 4: return (T) this._5;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}
	@Override
	@SuppressWarnings("unchecked")
	public <T> void setField(T value, int pos) {
		switch(pos) {
			case 0:
				this._1 = (T1) value;
				break;
			case 1:
				this._2 = (T2) value;
				break;
			case 2:
				this._3 = (T3) value;
				break;
			case 3:
				this._4 = (T4) value;
				break;
			case 4:
				this._5 = (T5) value;
				break;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	// -------------------------------------------------------------------------------------------------
	// standard utilities
	// -------------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return "(" + StringUtils.arrayAwareToString(this._1)
			+ ", " + StringUtils.arrayAwareToString(this._2)
			+ ", " + StringUtils.arrayAwareToString(this._3)
			+ ", " + StringUtils.arrayAwareToString(this._4)
			+ ", " + StringUtils.arrayAwareToString(this._5)
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

	private static final long[] offsets = new long[5];

	static {
		try {
			offsets[0] = UNSAFE.objectFieldOffset(Tuple5.class.getDeclaredField("_1"));
			offsets[1] = UNSAFE.objectFieldOffset(Tuple5.class.getDeclaredField("_2"));
			offsets[2] = UNSAFE.objectFieldOffset(Tuple5.class.getDeclaredField("_3"));
			offsets[3] = UNSAFE.objectFieldOffset(Tuple5.class.getDeclaredField("_4"));
			offsets[4] = UNSAFE.objectFieldOffset(Tuple5.class.getDeclaredField("_5"));
		} catch (Throwable t) {
			throw new RuntimeException("Could not initialize fast field accesses for tuple data type.");
		}
	}
}
