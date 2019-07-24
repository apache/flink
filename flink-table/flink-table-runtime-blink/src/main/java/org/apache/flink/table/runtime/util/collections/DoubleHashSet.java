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

import org.apache.flink.table.runtime.util.MurmurHashUtil;

/**
 * Double hash set.
 */
public class DoubleHashSet extends OptimizableHashSet {

	private double[] key;

	public DoubleHashSet(final int expected, final float f) {
		super(expected, f);
		this.key = new double[this.n + 1];
	}

	public DoubleHashSet(final int expected) {
		this(expected, DEFAULT_LOAD_FACTOR);
	}

	public DoubleHashSet() {
		this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
	}

	/**
	 * See {@link Double#equals(Object)}.
	 */
	public boolean add(final double k) {
		long longKey = Double.doubleToLongBits(k);
		if (longKey == 0L) {
			if (this.containsZero) {
				return false;
			}

			this.containsZero = true;
		} else {
			double[] key = this.key;
			int pos;
			long curr;
			if ((curr = Double.doubleToLongBits(key[pos = (int) MurmurHashUtil.fmix(longKey) & this.mask])) != 0L) {
				if (curr == longKey) {
					return false;
				}

				while ((curr = Double.doubleToLongBits(key[pos = pos + 1 & this.mask])) != 0L) {
					if (curr == longKey) {
						return false;
					}
				}
			}

			key[pos] = k;
		}

		if (this.size++ >= this.maxFill) {
			this.rehash(OptimizableHashSet.arraySize(this.size + 1, this.f));
		}

		return true;
	}

	/**
	 * See {@link Double#equals(Object)}.
	 */
	public boolean contains(final double k) {
		long longKey = Double.doubleToLongBits(k);
		if (longKey == 0L) {
			return this.containsZero;
		} else {
			double[] key = this.key;
			long curr;
			int pos;
			if ((curr = Double.doubleToLongBits(key[pos = (int) MurmurHashUtil.fmix(longKey) & this.mask])) == 0L) {
				return false;
			} else if (longKey == curr) {
				return true;
			} else {
				while ((curr = Double.doubleToLongBits(key[pos = pos + 1 & this.mask])) != 0L) {
					if (longKey == curr) {
						return true;
					}
				}

				return false;
			}
		}
	}

	private void rehash(final int newN) {
		double[] key = this.key;
		int mask = newN - 1;
		double[] newKey = new double[newN + 1];
		int i = this.n;

		int pos;
		for (int j = this.realSize(); j-- != 0; newKey[pos] = key[i]) {
			do {
				--i;
			} while (Double.doubleToLongBits(key[i]) == 0L);

			if (Double.doubleToLongBits(newKey[pos =
					(int) MurmurHashUtil.fmix(Double.doubleToLongBits(key[i])) & mask]) != 0L) {
				while (Double.doubleToLongBits(newKey[pos = pos + 1 & mask]) != 0L) {}
			}
		}

		this.n = newN;
		this.mask = mask;
		this.maxFill = OptimizableHashSet.maxFill(this.n, this.f);
		this.key = newKey;
	}

	@Override
	public void optimize() {
	}
}
