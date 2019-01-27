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

package org.apache.flink.runtime.util;

import org.apache.flink.util.Preconditions;

import java.util.Comparator;
import java.util.TreeSet;

/**
 * Copied from blink-rest-server.
 * A set based structure that accepts element and make it sorted with fixed size.
 * @param <E> The element.
 */
public class FixedSortedSet<E> extends TreeSet<E> {
	private final int maxSize;
	private final Comparator<? super E> comparator;

	public FixedSortedSet(int maxSize) {
		this(maxSize, null);
	}

	public FixedSortedSet(int maxSize, Comparator<? super E> comparator) {
		super(comparator);

		this.maxSize = maxSize;
		this.comparator = comparator;
	}

	@Override
	public boolean add(E e) {
		if (null == e) {
			throw new NullPointerException();
		}

		if (maxSize <= 0) {
			return false;
		}

		if (size() >= maxSize) {
			// remove the last one based on the comparator
			E last = last();
			if (last != null) {
				boolean canPut = false;

				if (comparator != null) {
					if (comparator.compare(last, e) > 0) {
						canPut = true;
					}
				} else {
					Preconditions.checkState(e instanceof Comparable,
						"Element should be comparable if there is no comparator provided.");

					@SuppressWarnings("unchecked")
					Comparable<? super E> comparable = (Comparable<? super E>) last;
					if (comparable.compareTo(e) > 0) {
						canPut = true;
					}
				}

				if (canPut) {
					pollLast();
					return super.add(e);
				}
			}
			return false;
		} else {
			return super.add(e);
		}
	}
}
