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

package org.apache.flink.api.java.tuple;

import org.apache.flink.annotation.Public;
import org.apache.flink.types.NullFieldException;


/**
 * The base class of all tuples. Tuples have a fix length and contain a set of fields,
 * which may all be of different types. Because Tuples are strongly typed, each distinct
 * tuple length is represented by its own class. Tuples exists with up to 25 fields and
 * are described in the classes {@link Tuple1} to {@link Tuple25}.
 *
 * <p>The fields in the tuples may be accessed directly a public fields, or via position (zero indexed)
 * {@link #getField(int)}.
 *
 * <p>Tuples are in principle serializable. However, they may contain non-serializable fields,
 * in which case serialization will fail.
 */
@Public
public abstract class Tuple implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	public static final int MAX_ARITY = 25;


	/**
	 * Gets the field at the specified position.
	 *
	 * @param pos The position of the field, zero indexed.
	 * @return The field at the specified position.
	 * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
	 */
	public abstract <T> T getField(int pos);

	/**
	 * Gets the field at the specified position, throws NullFieldException if the field is null. Used for comparing key fields.
	 *
	 * @param pos The position of the field, zero indexed.
	 * @return The field at the specified position.
	 * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
	 * @throws NullFieldException Thrown, if the field at pos is null.
	 */
	public <T> T getFieldNotNull(int pos){
		T field = getField(pos);
		if (field != null) {
			return field;
		} else {
			throw new NullFieldException(pos);
		}
	}

	/**
	 * Sets the field at the specified position.
	 *
	 * @param value The value to be assigned to the field at the specified position.
	 * @param pos The position of the field, zero indexed.
	 * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
	 */
	public abstract <T> void setField(T value, int pos);

	/**
	 * Gets the number of field in the tuple (the tuple arity).
	 *
	 * @return The number of fields in the tuple.
	 */
	public abstract int getArity();

	/**
	 * Shallow tuple copy.
	 * @return A new Tuple with the same fields as this.
	 */
	public abstract <T extends Tuple> T copy();

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the class corresponding to the tuple of the given arity (dimensions). For
	 * example, {@code getTupleClass(3)} will return the {@code Tuple3.class}.
	 *
	 * @param arity The arity of the tuple class to get.
	 * @return The tuple class with the given arity.
	 */
	@SuppressWarnings("unchecked")
	public static Class<? extends Tuple> getTupleClass(int arity) {
		if (arity < 0 || arity > MAX_ARITY) {
			throw new IllegalArgumentException("The tuple arity must be in [0, " + MAX_ARITY + "].");
		}
		return (Class<? extends Tuple>) CLASSES[arity];
	}

	// --------------------------------------------------------------------------------------------
	// The following lines are generated.
	// --------------------------------------------------------------------------------------------

	// BEGIN_OF_TUPLE_DEPENDENT_CODE
	// GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.
	public static Tuple newInstance(int arity) {
		switch (arity) {
			case 0: return Tuple0.INSTANCE;
			case 1: return new Tuple1();
			case 2: return new Tuple2();
			case 3: return new Tuple3();
			case 4: return new Tuple4();
			case 5: return new Tuple5();
			case 6: return new Tuple6();
			case 7: return new Tuple7();
			case 8: return new Tuple8();
			case 9: return new Tuple9();
			case 10: return new Tuple10();
			case 11: return new Tuple11();
			case 12: return new Tuple12();
			case 13: return new Tuple13();
			case 14: return new Tuple14();
			case 15: return new Tuple15();
			case 16: return new Tuple16();
			case 17: return new Tuple17();
			case 18: return new Tuple18();
			case 19: return new Tuple19();
			case 20: return new Tuple20();
			case 21: return new Tuple21();
			case 22: return new Tuple22();
			case 23: return new Tuple23();
			case 24: return new Tuple24();
			case 25: return new Tuple25();
			default: throw new IllegalArgumentException("The tuple arity must be in [0, " + MAX_ARITY + "].");
		}
	}

	private static final Class<?>[] CLASSES = new Class<?>[] {
		Tuple0.class, Tuple1.class, Tuple2.class, Tuple3.class, Tuple4.class, Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class, Tuple9.class, Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class, Tuple14.class, Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class, Tuple19.class, Tuple20.class, Tuple21.class, Tuple22.class, Tuple23.class, Tuple24.class, Tuple25.class
	};
	// END_OF_TUPLE_DEPENDENT_CODE
}
