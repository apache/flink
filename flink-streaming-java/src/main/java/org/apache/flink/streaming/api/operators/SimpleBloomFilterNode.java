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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * To be continued.
 */
public class SimpleBloomFilterNode implements TolerantFilterNode {

	private int capacity;
	private double fpp;
	private long ttl;
	private long deleteTS = Long.MAX_VALUE;

	SimpleBloomFilterNode() {
	}

	public SimpleBloomFilterNode(int capacity, double fpp, long ttl) {
	}

	@Override
	public boolean full() {
		return false;
	}

	@Override
	public long capacity() {
		int capacity = 0;
		return capacity;
	}

	@Override
	public long size() {
		int size = 0;
		return size;
	}

	@Override
	public long ttl() {
		return ttl;
	}

	@Override
	public double fpp() {
		return fpp;
	}

	public void reSetTtl() {
		this.deleteTS = Long.MAX_VALUE;
	}

	@Override
	public long deadline() {
		return deleteTS;
	}

	@Override
	public void add(byte[] content) {
	}

	@Override
	public boolean contains(byte[] content) {

		return false;
	}

	private int computeUnitCapacity(int capacity, int units) {
		return Math.floorDiv(capacity, units);
	}

	@Override
	public String toString() {
		return String.format("{c:%d s:%d}", capacity, size());
	}

	@Override
	public boolean equals(Object obj) {
		return false;
	}

	@Override
	public void snapshot(DataOutputView outputView) throws IOException {
	}

	@Override
	public void restore(DataInputView source) throws IOException {
	}
}
