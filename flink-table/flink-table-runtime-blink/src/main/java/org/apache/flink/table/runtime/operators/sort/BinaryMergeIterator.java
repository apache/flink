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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.runtime.operators.sort.MergeIterator;
import org.apache.flink.runtime.operators.sort.PartialOrderPriorityQueue;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Binary version of {@link MergeIterator}.
 * Use {@link RecordComparator} to compare record.
 */
public class BinaryMergeIterator<Entry> implements MutableObjectIterator<Entry> {

	// heap over the head elements of the stream
	private final PartialOrderPriorityQueue<HeadStream<Entry>> heap;
	private HeadStream<Entry> currHead;

	public BinaryMergeIterator(
			List<MutableObjectIterator<Entry>> iterators,
			List<Entry> reusableEntries,
			Comparator<Entry> comparator) throws IOException {
		checkArgument(iterators.size() == reusableEntries.size());
		this.heap = new PartialOrderPriorityQueue<>(
				(o1, o2) -> comparator.compare(o1.getHead(), o2.getHead()), iterators.size());
		for (int i = 0; i < iterators.size(); i++) {
			this.heap.add(new HeadStream<>(iterators.get(i), reusableEntries.get(i)));
		}
	}

	@Override
	public Entry next(Entry reuse) throws IOException {
		// Ignore reuse, because each HeadStream has its own reuse BinaryRow.
		return next();
	}

	@Override
	public Entry next() throws IOException {
		if (currHead != null) {
			if (!currHead.nextHead()) {
				this.heap.poll();
			} else {
				this.heap.adjustTop();
			}
		}

		if (this.heap.size() > 0) {
			currHead = this.heap.peek();
			return currHead.getHead();
		} else {
			return null;
		}
	}

	private static final class HeadStream<Entry> {

		private final MutableObjectIterator<Entry> iterator;
		private Entry head;

		private HeadStream(MutableObjectIterator<Entry> iterator, Entry head) throws IOException {
			this.iterator = iterator;
			this.head = head;
			if (!nextHead()) {
				throw new IllegalStateException();
			}
		}

		private Entry getHead() {
			return this.head;
		}

		private boolean nextHead() throws IOException {
			return (this.head = this.iterator.next(head)) != null;
		}
	}
}
