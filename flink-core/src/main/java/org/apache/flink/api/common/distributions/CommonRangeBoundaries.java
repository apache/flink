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
package org.apache.flink.api.common.distributions;

import org.apache.flink.api.common.typeutils.TypeComparator;

public class CommonRangeBoundaries<T> implements RangeBoundaries<T> {
	private Object[][] boundaries;

	public CommonRangeBoundaries(Object[][] boundaries) {
		this.boundaries = boundaries;
	}

	@Override
	public int getRangeIndex(T record, TypeComparator<T> typeComparator) {
		return binarySearch(record, typeComparator);
	}

	// Search the range index of input record.
	private int binarySearch(T record, TypeComparator<T> typeComparator) {
		int low = 0;
		int high = this.boundaries.length - 1;
		TypeComparator[] flatComparators = typeComparator.getFlatComparators();
		Object[] keys = new Object[flatComparators.length];
		typeComparator.extractKeys(record, keys, 0);

		while (low <= high) {
			final int mid = (low + high) >>> 1;
			final int result = compareKeys(flatComparators, keys, this.boundaries[mid]);

			if (result > 0) {
				low = mid + 1;
			} else if (result < 0) {
				high = mid - 1;
			} else {
				return mid;
			}
		}
		// key not found, but the low index is the target
		// bucket, since the boundaries are the upper bound
		return low;
	}

	private int compareKeys(TypeComparator[] flatComparators, Object[] keys, Object[] boundary) {
		if (flatComparators.length != keys.length || flatComparators.length != boundary.length) {
			throw new RuntimeException("Can not compare keys with boundary due to mismatched length.");
		}

		for (int i=0; i<flatComparators.length; i++) {
			int result = flatComparators[i].compare(keys[i], boundary[i]);
			if (result != 0) {
				return result;
			}
		}

		return 0;
	}
}
