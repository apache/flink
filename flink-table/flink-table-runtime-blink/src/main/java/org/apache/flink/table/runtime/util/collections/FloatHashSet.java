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
 * Float hash set.
 */
public class FloatHashSet extends OptimizableHashSet {

	private float[] key;

	public FloatHashSet(final int expected, final float f) {
		super(expected, f);
		this.key = new float[this.n + 1];
	}

	public FloatHashSet(final int expected) {
		this(expected, DEFAULT_LOAD_FACTOR);
	}

	public FloatHashSet() {
		this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
	}

	/**
	 * See {@link Float#equals(Object)}.
	 */
	public boolean add(final float k) {
		int intKey = Float.floatToIntBits(k);
		if (intKey == 0) {
			if (this.containsZero) {
				return false;
			}

			this.containsZero = true;
		} else {
			float[] key = this.key;
			int pos;
			int curr;
			if ((curr = Float.floatToIntBits(key[pos = MurmurHashUtil.fmix(intKey) & this.mask])) != 0) {
				if (curr == intKey) {
					return false;
				}

				while ((curr = Float.floatToIntBits(key[pos = pos + 1 & this.mask])) != 0) {
					if (curr == intKey) {
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
	 * See {@link Float#equals(Object)}.
	 */
	public boolean contains(final float k) {
		int intKey = Float.floatToIntBits(k);
		if (intKey == 0) {
			return this.containsZero;
		} else {
			float[] key = this.key;
			int curr;
			int pos;
			if ((curr = Float.floatToIntBits(key[pos = MurmurHashUtil.fmix(intKey) & this.mask])) == 0) {
				return false;
			} else if (intKey == curr) {
				return true;
			} else {
				while ((curr = Float.floatToIntBits(key[pos = pos + 1 & this.mask])) != 0) {
					if (intKey == curr) {
						return true;
					}
				}

				return false;
			}
		}
	}

	private void rehash(final int newN) {
		float[] key = this.key;
		int mask = newN - 1;
		float[] newKey = new float[newN + 1];
		int i = this.n;

		int pos;
		for (int j = this.realSize(); j-- != 0; newKey[pos] = key[i]) {
			do {
				--i;
			} while (Float.floatToIntBits(key[i]) == 0);

			if (Float.floatToIntBits(newKey[pos =
				MurmurHashUtil.fmix(Float.floatToIntBits(key[i])) & mask]) != 0) {
				while (Float.floatToIntBits(newKey[pos = pos + 1 & mask]) != 0) {}
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
