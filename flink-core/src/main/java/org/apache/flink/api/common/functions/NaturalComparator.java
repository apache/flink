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

package org.apache.flink.api.common.functions;

/**
 * A {@link Comparator} which compares objects by their natural orderings.
 *
 * @param <T> Type of the objects to be compared.
 */
public class NaturalComparator<T extends Comparable<T>> implements Comparator<T> {
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(T o1, T o2) {
		return o1.compareTo(o2);
	}

	@Override
	public boolean equals(Object o) {
		return (o == this) || (o != null && o.getClass() == getClass());
	}

	@Override
	public int hashCode() {
		return "NaturalComparator".hashCode();
	}

	@Override
	public String toString() {
		return "NaturalComparator";
	}
}

