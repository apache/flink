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

package org.apache.flink.util;

import org.apache.flink.annotation.Public;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Abstract base class for iterators that can split themselves into multiple disjoint
 * iterators. The union of these iterators returns the original iterator values.
 *
 * @param <T> The type of elements returned by the iterator.
 */
@Public
public abstract class SplittableIterator<T> implements Iterator<T>, Serializable {

	private static final long serialVersionUID = 200377674313072307L;

	/**
	 * Splits this iterator into a number disjoint iterators.
	 * The union of these iterators returns the original iterator values.
	 *
	 * @param numPartitions The number of iterators to split into.
	 * @return An array with the split iterators.
	 */
	public abstract Iterator<T>[] split(int numPartitions);

	/**
	 * Splits this iterator into <i>n</i> partitions and returns the <i>i-th</i> partition
	 * out of those.
	 *
	 * @param num The partition to return (<i>i</i>).
	 * @param numPartitions The number of partitions to split into (<i>n</i>).
	 * @return The iterator for the partition.
	 */
	public Iterator<T> getSplit(int num, int numPartitions) {
		if (numPartitions < 1 || num < 0 || num >= numPartitions) {
			throw new IllegalArgumentException();
		}

		return split(numPartitions)[num];
	}

	/**
	 * The maximum number of splits into which this iterator can be split up.
	 *
	 * @return The maximum number of splits into which this iterator can be split up.
	 */
	public abstract int getMaximumNumberOfSplits();
}
