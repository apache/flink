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

import org.apache.flink.types.NullFieldException;


/**
 * The base class of all tuples. Tuples have a fix length and contain a set of fields,
 * which may all be of different types. Because Tuples are strongly typed, each distinct
 * tuple length is represented by its own class. Tuples exists with up to 25 fields and
 * are described in the classes {@link Tuple1} to {@link Tuple25}.
 * <p>
 * The fields in the tuples may be accessed directly a public fields, or via position (zero indexed)
 * {@link #getField(int)}.
 * <p>
 * Tuples are in principle serializable. However, they may contain non-serializable fields,
 * in which case serialization will fail.
 */
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
}
