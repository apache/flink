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
 * A tuple with 18 fields. Tuples are strongly typed; each field may be of a separate type.
 * The fields of the tuple can be accessed directly as public fields (f0, f1, ...) or via their position
 * through the {@link #getField(int)} method. The tuple field positions start at zero.
 * <p>
 * Tuples are mutable types, meaning that their fields can be re-assigned. This allows functions that work
 * with Tuples to reuse objects in order to reduce pressure on the garbage collector.
 *
 * @see Tuple
 *
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 * @param <T2> The type of field 2
 * @param <T3> The type of field 3
 * @param <T4> The type of field 4
 * @param <T5> The type of field 5
 * @param <T6> The type of field 6
 * @param <T7> The type of field 7
 * @param <T8> The type of field 8
 * @param <T9> The type of field 9
 * @param <T10> The type of field 10
 * @param <T11> The type of field 11
 * @param <T12> The type of field 12
 * @param <T13> The type of field 13
 * @param <T14> The type of field 14
 * @param <T15> The type of field 15
 * @param <T16> The type of field 16
 * @param <T17> The type of field 17
 */
@SuppressWarnings({"restriction"})
public class Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> extends Tuple {

	private static final long serialVersionUID = 1L;

	/** Field 0 of the tuple. */
	public T0 f0;
	/** Field 1 of the tuple. */
	public T1 f1;
	/** Field 2 of the tuple. */
	public T2 f2;
	/** Field 3 of the tuple. */
	public T3 f3;
	/** Field 4 of the tuple. */
	public T4 f4;
	/** Field 5 of the tuple. */
	public T5 f5;
	/** Field 6 of the tuple. */
	public T6 f6;
	/** Field 7 of the tuple. */
	public T7 f7;
	/** Field 8 of the tuple. */
	public T8 f8;
	/** Field 9 of the tuple. */
	public T9 f9;
	/** Field 10 of the tuple. */
	public T10 f10;
	/** Field 11 of the tuple. */
	public T11 f11;
	/** Field 12 of the tuple. */
	public T12 f12;
	/** Field 13 of the tuple. */
	public T13 f13;
	/** Field 14 of the tuple. */
	public T14 f14;
	/** Field 15 of the tuple. */
	public T15 f15;
	/** Field 16 of the tuple. */
	public T16 f16;
	/** Field 17 of the tuple. */
	public T17 f17;

	/**
	 * Creates a new tuple where all fields are null.
	 */
	public Tuple18() {}

	/**
	 * Creates a new tuple and assigns the given values to the tuple's fields.
	 *
	 * @param value0 The value for field 0
	 * @param value1 The value for field 1
	 * @param value2 The value for field 2
	 * @param value3 The value for field 3
	 * @param value4 The value for field 4
	 * @param value5 The value for field 5
	 * @param value6 The value for field 6
	 * @param value7 The value for field 7
	 * @param value8 The value for field 8
	 * @param value9 The value for field 9
	 * @param value10 The value for field 10
	 * @param value11 The value for field 11
	 * @param value12 The value for field 12
	 * @param value13 The value for field 13
	 * @param value14 The value for field 14
	 * @param value15 The value for field 15
	 * @param value16 The value for field 16
	 * @param value17 The value for field 17
	 */
	public Tuple18(T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8, T9 value9, T10 value10, T11 value11, T12 value12, T13 value13, T14 value14, T15 value15, T16 value16, T17 value17) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
		this.f3 = value3;
		this.f4 = value4;
		this.f5 = value5;
		this.f6 = value6;
		this.f7 = value7;
		this.f8 = value8;
		this.f9 = value9;
		this.f10 = value10;
		this.f11 = value11;
		this.f12 = value12;
		this.f13 = value13;
		this.f14 = value14;
		this.f15 = value15;
		this.f16 = value16;
		this.f17 = value17;
	}

	@Override
	public int getArity() { return 18; }

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getField(int pos) {
		switch(pos) {
			case 0: return (T) this.f0;
			case 1: return (T) this.f1;
			case 2: return (T) this.f2;
			case 3: return (T) this.f3;
			case 4: return (T) this.f4;
			case 5: return (T) this.f5;
			case 6: return (T) this.f6;
			case 7: return (T) this.f7;
			case 8: return (T) this.f8;
			case 9: return (T) this.f9;
			case 10: return (T) this.f10;
			case 11: return (T) this.f11;
			case 12: return (T) this.f12;
			case 13: return (T) this.f13;
			case 14: return (T) this.f14;
			case 15: return (T) this.f15;
			case 16: return (T) this.f16;
			case 17: return (T) this.f17;
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
			case 4:
				this.f4 = (T4) value;
				break;
			case 5:
				this.f5 = (T5) value;
				break;
			case 6:
				this.f6 = (T6) value;
				break;
			case 7:
				this.f7 = (T7) value;
				break;
			case 8:
				this.f8 = (T8) value;
				break;
			case 9:
				this.f9 = (T9) value;
				break;
			case 10:
				this.f10 = (T10) value;
				break;
			case 11:
				this.f11 = (T11) value;
				break;
			case 12:
				this.f12 = (T12) value;
				break;
			case 13:
				this.f13 = (T13) value;
				break;
			case 14:
				this.f14 = (T14) value;
				break;
			case 15:
				this.f15 = (T15) value;
				break;
			case 16:
				this.f16 = (T16) value;
				break;
			case 17:
				this.f17 = (T17) value;
				break;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	/**
	 * Sets new values to all fields of the tuple.
	 *
	 * @param value0 The value for field 0
	 * @param value1 The value for field 1
	 * @param value2 The value for field 2
	 * @param value3 The value for field 3
	 * @param value4 The value for field 4
	 * @param value5 The value for field 5
	 * @param value6 The value for field 6
	 * @param value7 The value for field 7
	 * @param value8 The value for field 8
	 * @param value9 The value for field 9
	 * @param value10 The value for field 10
	 * @param value11 The value for field 11
	 * @param value12 The value for field 12
	 * @param value13 The value for field 13
	 * @param value14 The value for field 14
	 * @param value15 The value for field 15
	 * @param value16 The value for field 16
	 * @param value17 The value for field 17
	 */
	public void setFields(T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8, T9 value9, T10 value10, T11 value11, T12 value12, T13 value13, T14 value14, T15 value15, T16 value16, T17 value17) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
		this.f3 = value3;
		this.f4 = value4;
		this.f5 = value5;
		this.f6 = value6;
		this.f7 = value7;
		this.f8 = value8;
		this.f9 = value9;
		this.f10 = value10;
		this.f11 = value11;
		this.f12 = value12;
		this.f13 = value13;
		this.f14 = value14;
		this.f15 = value15;
		this.f16 = value16;
		this.f17 = value17;
	}


	// -------------------------------------------------------------------------------------------------
	// standard utilities
	// -------------------------------------------------------------------------------------------------

	/**
	 * Creates a string representation of the tuple in the form
	 * (f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17),
	 * where the individual fields are the value returned by calling {@link Object#toString} on that field.
	 * @return The string representation of the tuple.
	 */
	@Override
	public String toString() {
		return "(" + StringUtils.arrayAwareToString(this.f0)
			+ ", " + StringUtils.arrayAwareToString(this.f1)
			+ ", " + StringUtils.arrayAwareToString(this.f2)
			+ ", " + StringUtils.arrayAwareToString(this.f3)
			+ ", " + StringUtils.arrayAwareToString(this.f4)
			+ ", " + StringUtils.arrayAwareToString(this.f5)
			+ ", " + StringUtils.arrayAwareToString(this.f6)
			+ ", " + StringUtils.arrayAwareToString(this.f7)
			+ ", " + StringUtils.arrayAwareToString(this.f8)
			+ ", " + StringUtils.arrayAwareToString(this.f9)
			+ ", " + StringUtils.arrayAwareToString(this.f10)
			+ ", " + StringUtils.arrayAwareToString(this.f11)
			+ ", " + StringUtils.arrayAwareToString(this.f12)
			+ ", " + StringUtils.arrayAwareToString(this.f13)
			+ ", " + StringUtils.arrayAwareToString(this.f14)
			+ ", " + StringUtils.arrayAwareToString(this.f15)
			+ ", " + StringUtils.arrayAwareToString(this.f16)
			+ ", " + StringUtils.arrayAwareToString(this.f17)
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

	private static final long[] offsets = new long[18];

	static {
		try {
			offsets[0] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f0"));
			offsets[1] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f1"));
			offsets[2] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f2"));
			offsets[3] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f3"));
			offsets[4] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f4"));
			offsets[5] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f5"));
			offsets[6] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f6"));
			offsets[7] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f7"));
			offsets[8] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f8"));
			offsets[9] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f9"));
			offsets[10] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f10"));
			offsets[11] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f11"));
			offsets[12] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f12"));
			offsets[13] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f13"));
			offsets[14] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f14"));
			offsets[15] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f15"));
			offsets[16] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f16"));
			offsets[17] = UNSAFE.objectFieldOffset(Tuple18.class.getDeclaredField("f17"));
		} catch (Throwable t) {
			throw new RuntimeException("Could not initialize fast field accesses for tuple data type.");
		}
	}
}
