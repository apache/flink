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

import org.apache.flink.table.util.MurmurHashUtil;

/**
 * Byte hash set.
 */
public class ByteHashSet extends OptimizableHashSet {

	private byte[] key;

	private byte min = Byte.MAX_VALUE;
	private byte max = Byte.MIN_VALUE;

	public ByteHashSet(final int expected, final float f) {
		super(expected, f);
		this.key = new byte[this.n + 1];
	}

	public ByteHashSet(final int expected) {
		this(expected, DEFAULT_LOAD_FACTOR);
	}

	public ByteHashSet() {
		this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
	}

	public boolean add(final byte k) {
		if (k == 0) {
			if (this.containsZero) {
				return false;
			}

			this.containsZero = true;
		} else {
			byte[] key = this.key;
			int pos;
			byte curr;
			if ((curr = key[pos = MurmurHashUtil.fmix(k) & this.mask]) != 0) {
				if (curr == k) {
					return false;
				}

				while ((curr = key[pos = pos + 1 & this.mask]) != 0) {
					if (curr == k) {
						return false;
					}
				}
			}

			key[pos] = k;
		}

		if (this.size++ >= this.maxFill) {
			this.rehash(OptimizableHashSet.arraySize(this.size + 1, this.f));
		}

		if (k < min) {
			min = k;
		}
		if (k > max) {
			max = k;
		}
		return true;
	}

	public boolean contains(final byte k) {
		if (isDense) {
			return k >= min && k <= max && used[k - min];
		} else {
			if (k == 0) {
				return this.containsZero;
			} else {
				byte[] key = this.key;
				byte curr;
				int pos;
				if ((curr = key[pos = MurmurHashUtil.fmix(k) & this.mask]) == 0) {
					return false;
				} else if (k == curr) {
					return true;
				} else {
					while ((curr = key[pos = pos + 1 & this.mask]) != 0) {
						if (k == curr) {
							return true;
						}
					}

					return false;
				}
			}
		}
	}

	private void rehash(final int newN) {
		byte[] key = this.key;
		int mask = newN - 1;
		byte[] newKey = new byte[newN + 1];
		int i = this.n;

		int pos;
		for (int j = this.realSize(); j-- != 0; newKey[pos] = key[i]) {
			do {
				--i;
			} while(key[i] == 0);

			if (newKey[pos = MurmurHashUtil.fmix(key[i]) & mask] != 0) {
				while (newKey[pos = pos + 1 & mask] != 0) {}
			}
		}

		this.n = newN;
		this.mask = mask;
		this.maxFill = OptimizableHashSet.maxFill(this.n, this.f);
		this.key = newKey;
	}

	@Override
	public void optimize() {
		int range = max - min;
		if (range >= 0 && (range < key.length || range < OptimizableHashSet.DENSE_THRESHOLD)) {
			this.used = new boolean[max - min + 1];
			for (byte v : key) {
				if (v != 0) {
					used[v - min] = true;
				}
			}
			if (containsZero) {
				used[-min] = true;
			}
			isDense = true;
			key = null;
		}
	}
}
