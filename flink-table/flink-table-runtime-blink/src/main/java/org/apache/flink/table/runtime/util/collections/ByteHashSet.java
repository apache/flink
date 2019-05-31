/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util.collections;

/**
 * Byte hash set.
 */
public class ByteHashSet extends OptimizableHashSet {

	private final byte min = Byte.MIN_VALUE;
	private final byte max = Byte.MAX_VALUE;

	public ByteHashSet(final int expected, final float f) {
		super(expected, f);
		used = new boolean[max - min + 1];
	}

	public ByteHashSet(final int expected) {
		this(expected, DEFAULT_LOAD_FACTOR);
	}

	public ByteHashSet() {
		this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
	}

	public boolean add(final byte k) {
		if (used[k - min]) {
			return false;
		} else {
			return used[k - min] = true;
		}
	}

	public boolean contains(final byte k) {
		return used[k - min];
	}

	@Override
	public void optimize() {
	}
}
