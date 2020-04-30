/*
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

package org.apache.flink.runtime.state;

/**
 * This interface works similar to {@link Comparable} and is used to prioritize between two objects. The main difference
 * between this interface and {@link Comparable} is it is not require to follow the usual contract between that
 * {@link Comparable#compareTo(Object)} and {@link Object#equals(Object)}. The contract of this interface is:
 * When two objects are equal, they indicate the same priority, but indicating the same priority does not require that
 * both objects are equal.
 *
 * @param <T> type of the compared objects.
 */
@FunctionalInterface
public interface PriorityComparator<T> {

	PriorityComparator<? extends PriorityComparable<Object>> FOR_PRIORITY_COMPARABLE_OBJECTS = PriorityComparable::comparePriorityTo;

	/**
	 * Compares two objects for priority. Returns a negative integer, zero, or a positive integer as the first
	 * argument has lower, equal to, or higher priority than the second.
	 * @param left left operand in the comparison by priority.
	 * @param right left operand in the comparison by priority.
	 * @return a negative integer, zero, or a positive integer as the first argument has lower, equal to, or higher
	 * priority than the second.
	 */
	int comparePriority(T left, T right);

	@SuppressWarnings("unchecked")
	static <T extends PriorityComparable<?>> PriorityComparator<T> forPriorityComparableObjects() {
		return (PriorityComparator<T>) FOR_PRIORITY_COMPARABLE_OBJECTS;
	}
}
