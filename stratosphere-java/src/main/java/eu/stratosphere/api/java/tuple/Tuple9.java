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
public class Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T9> extends Tuple {

	private static final long serialVersionUID = 1L;

	private T1 _1;
	private T2 _2;
	private T3 _3;
	private T4 _4;
	private T5 _5;
	private T6 _6;
	private T7 _7;
	private T8 _8;
	private T9 _9;

	public Tuple9() {}

	public Tuple9(T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8, T9 value9) {
		this._1 = value1;
		this._2 = value2;
		this._3 = value3;
		this._4 = value4;
		this._5 = value5;
		this._6 = value6;
		this._7 = value7;
		this._8 = value8;
		this._9 = value9;
	}

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
	public T6 T6() {
		return this._6;
	}
	public T7 T7() {
		return this._7;
	}
	public T8 T8() {
		return this._8;
	}
	public T9 T9() {
		return this._9;
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
	public void T6(T6 value) {
		this._6 = value;
	}
	public void T7(T7 value) {
		this._7 = value;
	}
	public void T8(T8 value) {
		this._8 = value;
	}
	public void T9(T9 value) {
		this._9 = value;
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
			case 5: return (T) this._6;
			case 6: return (T) this._7;
			case 7: return (T) this._8;
			case 8: return (T) this._9;
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
			case 5:
				this._6 = (T6) value;
				break;
			case 6:
				this._7 = (T7) value;
				break;
			case 7:
				this._8 = (T8) value;
				break;
			case 8:
				this._9 = (T9) value;
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
			+ ", " + StringUtils.arrayAwareToString(this._6)
			+ ", " + StringUtils.arrayAwareToString(this._7)
			+ ", " + StringUtils.arrayAwareToString(this._8)
			+ ", " + StringUtils.arrayAwareToString(this._9)
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

	private static final long[] offsets = new long[9];

	static {
		try {
			offsets[0] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_1"));
			offsets[1] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_2"));
			offsets[2] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_3"));
			offsets[3] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_4"));
			offsets[4] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_5"));
			offsets[5] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_6"));
			offsets[6] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_7"));
			offsets[7] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_8"));
			offsets[8] = UNSAFE.objectFieldOffset(Tuple9.class.getDeclaredField("_9"));
		} catch (Throwable t) {
			throw new RuntimeException("Could not initialize fast field accesses for tuple data type.");
		}
	}
}
