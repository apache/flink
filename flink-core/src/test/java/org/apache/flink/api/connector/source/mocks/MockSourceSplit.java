/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.api.connector.source.mocks;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Simple Mock SourceSplit for unit test. The implementation of this class is just
 * an in-memory queue. The values are integers and each value has an associated
 * index which is its position in the queue over the entire life cycle of the split.
 * For example, a value with index K means it is the K-th element that was
 * polled out of the queue since the creation of this split.
 */
public class MockSourceSplit implements SourceSplit, Serializable {
	private final int id;
	private final BlockingQueue<Integer> records;
	private final int endIndex;
	private int index;

	public MockSourceSplit(int id) {
		this(id, 0);
	}

	public MockSourceSplit(int id, int startingIndex) {
		this(id, startingIndex, Integer.MAX_VALUE);
	}

	public MockSourceSplit(int id, int startingIndex, int endIndex) {
		this.id = id;
		this.endIndex = endIndex;
		this.index = startingIndex;
		this.records = new LinkedBlockingQueue<>();
	}

	@Override
	public String splitId() {
		return Integer.toString(id);
	}

	public int index() {
		return index;
	}

	public int endIndex() {
		return endIndex;
	}

	public boolean isFinished() {
		return index == endIndex;
	}

	/**
	 * Get the next element. Block if asked.
	 */
	public int[] getNext(boolean blocking) throws InterruptedException {
		Integer value = blocking ? records.take() : records.poll();
		return value == null ? null : new int[]{value, index++};
	}

	/**
	 * Add a record to this split.
	 */
	public void addRecord(int record) {
		if (!records.offer(record)) {
			throw new IllegalStateException("Failed to add record to split.");
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, records.toArray(new Integer[0]), endIndex, index);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MockSourceSplit)) {
			return false;
		}
		MockSourceSplit that = (MockSourceSplit) obj;
		return
				id == that.id &&
				index == that.index &&
				Arrays.equals(records.toArray(new Integer[0]), that.records.toArray(new Integer[0])) &&
				endIndex == that.endIndex;
	}
}
