/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.vector;

import java.io.Serializable;
import java.util.Arrays;


/**
 * ColumnVector contains the shared structure for the sub-types,
 * including NULL information, and whether this vector
 * repeats, i.e. has all values the same, so only the first
 * one is set. This is used to accelerate query performance
 * by handling a whole vector in O(1) time when applicable.
 *
 * <p>The fields are public by design since this is a performance-critical
 * structure that is used in the inner loop of query execution.
 */
public abstract class ColumnVector implements Serializable {
	private static final long serialVersionUID = 5340018531388047747L;
	/*
	 * If hasNulls is true, then this array contains true if the value
	 * is null, otherwise false. The array is always allocated, so a batch can be re-used
	 * later and nulls added.
	 */
	public boolean[] isNull;
	// If the whole column vector has no nulls, this is true, otherwise false.
	public boolean noNulls;

	/**
	 * The Dictionary for this column.
	 * If it's not null, will be used to decode the value in get().
	 */
	protected Dictionary dictionary;

	/**
	 * Reusable column for ids of dictionary.
	 */
	protected IntegerColumnVector dictionaryIds;

	/**
	 * Constructor for super-class ColumnVector. This is not called directly,
	 * but used to initialize inherited fields.
	 *
	 * @param len Vector length
	 */
	public ColumnVector(int len) {
		isNull = new boolean[len];
		noNulls = true;
	}

	public abstract Object get(int index);

	/**
	 * Resets the column to default state.
	 * - fills the isNull array with false.
	 * - sets noNulls to true.
	 * - sets isRepeating to false.
	 */
	public void reset() {
		if (!noNulls) {
			Arrays.fill(isNull, false);
		}
		noNulls = true;
	}

	/**
	 * Set the element in this column vector from the given input vector.
	 * This method can assume that the output does not have isRepeating set.
	 */
	public abstract void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector);

	/**
	 * Initialize the column vector. This method can be overridden by specific column vector types.
	 * Use this method only if the individual type of the column vector is not known, otherwise its
	 * preferable to call specific initialization methods.
	 */
	public void init() {
		// Do nothing by default
	}

	public void copySelected(boolean selectedInUse, int[] sel, int size, ColumnVector output) {
		// Copy nulls over if needed
		if (!noNulls) {
			if (selectedInUse) {
				for (int j = 0; j < size; j++) {
					int i = sel[j];
					output.isNull[i] = isNull[i];
				}
			} else {
				System.arraycopy(isNull, 0, output.isNull, 0, size);
			}
		}
	}

	/**
	 * Shallow copy of the contents of this vector to the other vector;
	 * replaces other vector's values.
	 */
	public void shallowCopyTo(ColumnVector otherCv) {
		otherCv.isNull = isNull;
		otherCv.noNulls = noNulls;
	}

	/**
	 * Reserve a integer column for ids of dictionary.
	 */
	public IntegerColumnVector reserveDictionaryIds(int capacity) {
		if (dictionaryIds == null) {
			dictionaryIds = new IntegerColumnVector(capacity);
		} else {
			dictionaryIds.reset();
		}
		return dictionaryIds;
	}

	/**
	 * Update the dictionary.
	 */
	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	/**
	 * Returns true if this column has a dictionary.
	 */
	public boolean hasDictionary() {
		return this.dictionary != null;
	}

	/**
	 * Returns the underlying integer column for ids of dictionary.
	 */
	public IntegerColumnVector getDictionaryIds() {
		return dictionaryIds;
	}
}
