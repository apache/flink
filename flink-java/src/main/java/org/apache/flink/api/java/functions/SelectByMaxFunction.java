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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * Function that enables selection by maximal value of a field.
 * @param <T>
 */
@Internal
public class SelectByMaxFunction<T extends Tuple> implements ReduceFunction<T> {
	private static final long serialVersionUID = 1L;

	// Fields which are used as KEYS
	private int[] fields;

	/**
	 * Constructor which is overwriting the default constructor.
	 * @param type Types of tuple whether to check if given fields are key types.
	 * @param fields Array of integers which are used as key for comparison. The order of indexes
	 * is regarded in the reduce function. First index has highest priority and last index has
	 * least priority.
	 */
	public SelectByMaxFunction(TupleTypeInfo<T> type, int... fields) {
		this.fields = fields;

		// Check correctness of each position
		for (int field : fields) {
			// Is field inside array
			if (field < 0 || field >= type.getArity()) {
				throw new IndexOutOfBoundsException(
						"MinReduceFunction field position " + field + " is out of range.");
			}

			// Check whether type is comparable
			if (!type.getTypeAt(field).isKeyType()) {
				throw new java.lang.IllegalArgumentException(
						"MinReduceFunction supports only key(Comparable) types.");
			}

		}
	}

	/**
	 * Reduce implementation, returns bigger tuple or value1 if both tuples are
	 * equal. Comparison highly depends on the order and amount of fields chosen
	 * as indices. All given fields (at construction time) are checked in the same
	 * order as defined (at construction time). If both tuples are equal in one
	 * index, the next index is compared. Or if no next index is available value1
	 * is returned.
	 * The tuple which has a bigger value at one index will be returned.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public T reduce(T value1, T value2) throws Exception {

		for (int index = 0; index < fields.length; index++) {
			// Save position of compared key
			int position = this.fields[index];

			// Get both values - both implement comparable
			Comparable comparable1 = value1.getFieldNotNull(position);
			Comparable comparable2 = value2.getFieldNotNull(position);

			// Compare values
			int comp = comparable1.compareTo(comparable2);
			// If comp is bigger than 0 comparable 1 is bigger.
			// Return the smaller value.
			if (comp > 0) {
				return value1;
			} else if (comp < 0) {
				return value2;
			}
		}
		return value1;
	}
}
