/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.common.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;

/**
 * Wrapper that is used to store elements for which we have a TypeComparator in a Hash Map.
 */
@Internal
public class TypeComparable<T> {
	private final T elem;
	private final TypeComparator<T> comparator;
	private final int hashCode;

	public TypeComparable(T elem, TypeComparator<T> comparator) {
		this.elem = elem;
		this.comparator = comparator;
		this.hashCode = comparator.hash(elem);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof TypeComparable)) {
			return false;
		}
		@SuppressWarnings("unchecked")
		TypeComparable<T> other = (TypeComparable<T>) o;
		return comparator.compare(elem, other.elem) == 0;
	}
}
