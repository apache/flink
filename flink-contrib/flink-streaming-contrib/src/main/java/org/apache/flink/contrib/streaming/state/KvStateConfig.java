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

package org.apache.flink.contrib.streaming.state;

import java.io.Serializable;

public class KvStateConfig<T extends KvStateConfig<T>> implements Serializable {

	private static final long serialVersionUID = 1L;

	// Dedault properties
	protected int kvStateCacheSize;
	protected double maxKvEvictFraction;

	protected int bfExpectedInserts = -1;
	protected double bfFPP = -1;

	protected CompactionStrategy ct = CompactionStrategy.disabled();

	public KvStateConfig(int kvStateCacheSize, double maxCacheEvictFraction) {
		this.kvStateCacheSize = kvStateCacheSize;
		this.maxKvEvictFraction = maxCacheEvictFraction;
	}

	public KvStateConfig() {
		this(10000, 0.1);
	}

	/**
	 * The maximum number of key-value pairs stored in one task instance's cache
	 * before evicting to the underlying storage layer.
	 *
	 */
	public int getKvCacheSize() {
		return kvStateCacheSize;
	}

	/**
	 * Set the maximum number of key-value pairs stored in one task instance's
	 * cache before evicting to the underlying storage layer. When the cache is
	 * full the N least recently used keys will be evicted to the storage, where
	 * N = maxKvEvictFraction*KvCacheSize.
	 *
	 */
	@SuppressWarnings("unchecked")
	public T setKvCacheSize(int size) {
		kvStateCacheSize = size;
		return (T) this;
	}

	/**
	 * Sets the maximum fraction of key-value states evicted from the cache if
	 * the cache is full.
	 */
	@SuppressWarnings("unchecked")
	public T setMaxKvCacheEvictFraction(double fraction) {
		if (fraction > 1 || fraction <= 0) {
			throw new RuntimeException("Must be a number between 0 and 1");
		} else {
			maxKvEvictFraction = fraction;
		}
		return (T) this;
	}

	/**
	 * The maximum fraction of key-value states evicted from the cache if the
	 * cache is full.
	 */
	public double getMaxKvCacheEvictFraction() {
		return maxKvEvictFraction;
	}

	/**
	 * The number of elements that will be evicted when the cache is full.
	 * 
	 */
	public int getNumElementsToEvict() {
		return (int) Math.ceil(getKvCacheSize() * getMaxKvCacheEvictFraction());
	}

	@SuppressWarnings("unchecked")
	public T enableBloomFilter(int expectedInsertions, double fpp) {
		this.bfExpectedInserts = expectedInsertions;
		this.bfFPP = fpp;
		return (T) this;
	}

	public boolean hasBloomFilter() {
		return bfExpectedInserts > 0;
	}

	public int getBloomFilterExpectedInserts() {
		return bfExpectedInserts;
	}

	public double getBloomFilterFPP() {
		return bfFPP;
	}

	@SuppressWarnings("unchecked")
	public T setCompactionStrategy(CompactionStrategy ct) {
		this.ct = ct;
		return (T) this;
	}

	public CompactionStrategy getCompactionStrategy() {
		return ct;
	}

}
