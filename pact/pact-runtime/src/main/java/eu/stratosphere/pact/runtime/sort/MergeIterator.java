/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * @author Erik Nijkamp
 * @param <K>
 * @param <V>
 */
public class MergeIterator<K extends Key, V extends Value> implements Iterator<KeyValuePair<K, V>> {
	private final class HeadStream {
		private final Iterator<KeyValuePair<K, V>> iterator;

		private KeyValuePair<K, V> head;

		public HeadStream(Iterator<KeyValuePair<K, V>> iterator) {
			this.iterator = iterator;
			if (!nextHead())
				throw new IllegalStateException();
		}

		public KeyValuePair<K, V> getHead() {
			return head;
		}

		public boolean nextHead() {
			if (iterator.hasNext()) {
				head = iterator.next();
				return true;
			} else {
				return false;
			}
		}
	}

	private final class HeadStreamComparator implements Comparator<HeadStream> {
		/**
		 * The comparator providing the comparison operator for key-value-pairs.
		 */
		private final Comparator<K> comparator;

		public HeadStreamComparator(Comparator<K> comparator) {
			this.comparator = comparator;
		}

		@Override
		public int compare(HeadStream o1, HeadStream o2) {
			return comparator.compare(o1.getHead().getKey(), o2.getHead().getKey());
		}
	}

	private final PartialOrderPriorityQueue<HeadStream> heap;

	public MergeIterator(List<Iterator<KeyValuePair<K, V>>> iterators, Comparator<K> comparator) {
		this.heap = new PartialOrderPriorityQueue<HeadStream>(new HeadStreamComparator(comparator), iterators.size());
		for (Iterator<KeyValuePair<K, V>> iterator : iterators) {
			heap.add(new HeadStream(iterator));
		}
	}

	@Override
	public boolean hasNext() {
		return heap.size() != 0;
	}

	@Override
	public KeyValuePair<K, V> next() {
		// get the smallest element
		HeadStream top = heap.peek();
		KeyValuePair<K, V> head = top.getHead();

		// read an element
		try {
			if (!top.nextHead()) {
				heap.poll();
			}
		} catch (Exception e) {
			throw new RuntimeException("Reading an element from the stream failed.", e);
		}

		heap.adjustTop();

		return head;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
