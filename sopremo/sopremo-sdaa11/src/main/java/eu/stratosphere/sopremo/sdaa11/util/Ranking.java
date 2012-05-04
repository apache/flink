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
package eu.stratosphere.sopremo.sdaa11.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author skruse
 * 
 */
public class Ranking<T> implements Iterable<T> {

	private final int capacity;
	private Item<T> head, tail;
	private int size;

	public Ranking(final int capacity) {
		if (capacity < 2)
			throw new IllegalArgumentException("Capacity must be >= 2");
		this.capacity = capacity;
	}
	
	protected boolean isHigher(int rank1, int rank2) {
		return rank1 < rank2;
	}
	
	public Item<T> insert(Item<T> item) {
		return insert(item.content, item.rank);
	}

	public Item<T> insert(final T content, final int rank) {
		this.size++;
		if (this.size == 1)
			this.head = this.tail = new Item<T>(content, rank);
		else if (isHigher(rank, head.rank)) {
			final Item<T> newHead = new Item<T>(content, rank);
			newHead.linkTo(this.head);
			this.head = newHead;

		} else {
			Item<T> current = this.head;
			while (current.next != null && isHigher(current.next.rank, rank))
				current = current.next;
			final Item<T> newElement = new Item<T>(content, rank);
			final Item<T> follower = current.next;
			current.linkTo(newElement);
			newElement.linkTo(follower);
			if (newElement.next == null)
				this.tail = newElement;
		}
		return this.size > this.capacity ? this.removeTail() : null;
	}

	private Item<T> removeTail() {
		final Item<T> oldTail = this.tail;
		if (this.tail == this.head)
			this.tail = this.head = null;
		else if (this.tail != null) {
			this.tail = this.tail.prev;
			if (tail != null) {
				tail.next = null;
			}
		}
		this.size--;
		return oldTail;
	}

	public void clear() {
		this.head = this.tail = null;
		this.size = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<T> iterator() {
		return new RankingIterator();
	}

	public Object[] toArray() {
		final Object[] array = new Object[size];
		int index = 0;
		for (Item<T> elem = this.head; elem != null; elem = elem.next, index++)
			array[index] = elem.content;
		return array;
	}

	public List<T> toList() {
		List<T> list = new ArrayList<T>(size);
		for (T member : this) {
			list.add(member);
		}
		return list;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("Ranking[cap=").append(this.capacity).append("|");
		String separator = "";
		for (Item<T> current = this.head; current != null; current = current.next) {
			sb.append(separator).append(current);
			separator = ",";
		}
		sb.append("]");
		return sb.toString();
	}

	/**
	 * 
	 */
	public static class Item<T> {
		private Item<T> next, prev;
		private final T content;
		private final int rank;

		private Item(final T content, final int rank) {
			this.content = content;
			this.rank = rank;
		}

		private void linkTo(final Item<T> other) {
			this.next = other;
			if (other != null)
				other.prev = this;
		}

		/**
		 * Returns the content.
		 * 
		 * @return the content
		 */
		public T getContent() {
			return this.content;
		}

		/**
		 * Returns the rank.
		 * 
		 * @return the rank
		 */
		public int getRank() {
			return this.rank;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "Element[" + this.rank + "," + this.content + "]";
		}
	}

	private class RankingIterator implements Iterator<T> {
		private Item<T> current;

		private RankingIterator() {
			this.current = Ranking.this.head;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return this.current != null;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.util.Iterator#next()
		 */
		@Override
		public T next() {
			final T next = this.current.content;
			this.current = this.current.next;
			return next;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.util.Iterator#remove()
		 */
		@Override
		public void remove() {
			throw new UnsupportedOperationException("Remove not supported");
		}
	}

}
