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

package org.apache.flink.table.runtime.operators.window.grouping;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.util.RowIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A jvm heap implementation of {@link WindowsGrouping}, which uses a linked list to buffer
 * all the inputs of a keyed group belonging to the same window.
 * It is designed to have a capacity limit to avoid JVM OOM and reduce GC pauses.
 */
public class HeapWindowsGrouping extends WindowsGrouping {

	private LinkedList<BinaryRowData> buffer;

	private final int maxSizeLimit;

	private int evictLimitIndex;

	private Iterator<BinaryRowData> iterator;

	public HeapWindowsGrouping(int maxSizeLimit, long windowSize, long slideSize, int timeIndex, boolean isDate) {
		this(maxSizeLimit, 0L, windowSize, slideSize, timeIndex, isDate);
	}

	public HeapWindowsGrouping(
			int maxSizeLimit, long offset, long windowSize, long slideSize, int timeIndex, boolean isDate) {
		super(offset, windowSize, slideSize, timeIndex, isDate);
		this.maxSizeLimit = maxSizeLimit;
		this.evictLimitIndex = 0;
		this.buffer = new LinkedList<>();
	}

	@Override
	protected void resetBuffer() {
		buffer.clear();
		evictLimitIndex = 0;
		iterator = null;
	}

	@Override
	protected void onBufferEvict(int limitIndex) {
		while (evictLimitIndex < limitIndex) {
			buffer.removeFirst();
			evictLimitIndex++;
		}
	}

	@Override
	protected void addIntoBuffer(BinaryRowData input) throws IOException {
		if (buffer.size() >= maxSizeLimit) {
			throw new IOException("HeapWindowsGrouping out of memory, element size limit " + maxSizeLimit);
		}
		buffer.add(input);
	}

	@Override
	protected RowIterator<BinaryRowData> newBufferIterator(int startIndex) {
		iterator = buffer.subList(startIndex - evictLimitIndex, buffer.size()).iterator();
		return new BufferIterator(iterator);
	}

	@Override
	public void close() throws IOException {
		buffer = null;
	}

	private final class BufferIterator implements RowIterator<BinaryRowData> {
		private final Iterator<BinaryRowData> iterator;
		private BinaryRowData next;

		BufferIterator(Iterator<BinaryRowData> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean advanceNext() {
			if (iterator.hasNext()) {
				next = iterator.next();
				return true;
			} else {
				next = null;
				return false;
			}
		}

		@Override
		public BinaryRowData getRow() {
			return next;
		}
	}
}
