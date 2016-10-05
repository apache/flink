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


package org.apache.flink.runtime.operators.sort;

/**
 */
public interface IndexedSortable {

	/**
	 * Compare items at the given addresses consistent with the semantics of
	 * {@link java.util.Comparator#compare(Object, Object)}.
	 */
	int compare(int i, int j);

	/**
	 * Compare records at the given addresses consistent with the semantics of
	 * {@link java.util.Comparator#compare(Object, Object)}.

	 * @param segmentNumberI index of memory segment containing first record
	 * @param segmentOffsetI offset into memory segment containing first record
	 * @param segmentNumberJ index of memory segment containing second record
	 * @param segmentOffsetJ offset into memory segment containing second record
	 * @return a negative integer, zero, or a positive integer as the
	 *         first argument is less than, equal to, or greater than the
	 *         second.
	 */
	int compare(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ);

	/**
	 * Swap items at the given addresses.
	 */
	void swap(int i, int j);

	/**
	 * Swap records at the given addresses.
	 *
	 * @param segmentNumberI index of memory segment containing first record
	 * @param segmentOffsetI offset into memory segment containing first record
	 * @param segmentNumberJ index of memory segment containing second record
	 * @param segmentOffsetJ offset into memory segment containing second record
	 */
	void swap(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ);

	/**
	 * Gets the number of elements in the sortable.
	 * 
	 * @return The number of elements.
	 */
	int size();

	/**
	 * Gets the size of each record, the number of bytes separating the head
	 * of successive records.
	 *
	 * @return The record size
	 */
	int recordSize();

	/**
	 * Gets the number of elements in each memory segment.
	 *
	 * @return The number of records per segment
	 */
	int recordsPerSegment();
}
