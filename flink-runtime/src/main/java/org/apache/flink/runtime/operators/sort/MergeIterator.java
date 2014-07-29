/**
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


package org.apache.flink.runtime.operators.sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.MutableObjectIterator;

/**
 * An iterator that returns a sorted merge of the sequences of elements from a
 * set of iterators, assuming those sequences are ordered themselves.
 * The iterators to be merged are kept internally as a heap, making each access
 * to the next smallest element logarithmic in complexity, with respect to the
 * number of streams to be merged.
 * The order among the elements is established using the methods from the
 * {@link TypeComparator} class, specifically {@link TypeComparator#setReference(Object)}
 * and {@link TypeComparator#compareToReference(TypeComparator)}.
 */
public class MergeIterator<E> implements MutableObjectIterator<E> {
	
	private final PartialOrderPriorityQueue<HeadStream<E>> heap;	// heap over the head elements of the stream
	
	private final TypeSerializer<E> serializer;
	
	/**
	 * @param iterators
	 * @param serializer
	 * @param comparator
	 * @throws IOException
	 */
	public MergeIterator(List<MutableObjectIterator<E>> iterators,
			TypeSerializer<E> serializer, TypeComparator<E> comparator)
	throws IOException
	{
		this.heap = new PartialOrderPriorityQueue<HeadStream<E>>(new HeadStreamComparator<E>(), iterators.size());
		this.serializer = serializer;
		
		for (MutableObjectIterator<E> iterator : iterators) {
			this.heap.add(new HeadStream<E>(iterator, serializer, comparator.duplicate()));
		}
	}

	/**
	 * Gets the next smallest element, with respect to the definition of order implied by
	 * the {@link TypeSerializer} provided to this iterator.
	 * 
	 * @param reuse The object into which the result is put. The contents of the reuse object
	 *               is only valid after this method, if the method returned true. Otherwise
	 *               the contents is undefined.
	 * @return True, if the iterator had another element, false otherwise. 
	 * 
	 * @see org.apache.flink.util.MutableObjectIterator#next(java.lang.Object)
	 */
	@Override
	public E next(E reuse) throws IOException
	{
		if (this.heap.size() > 0) {
			// get the smallest element
			final HeadStream<E> top = this.heap.peek();
			reuse = this.serializer.copy(top.getHead(), reuse);
			
			// read an element
			if (!top.nextHead()) {
				this.heap.poll();
			} else {
				this.heap.adjustTop();
			}
			return reuse;
		}
		else {
			return null;
		}
	}

	// ============================================================================================
	//                      Internal Classes that wrap the sorted input streams
	// ============================================================================================
	
	private static final class HeadStream<E>
	{
		private final MutableObjectIterator<E> iterator;
		
		private final TypeComparator<E> comparator;
		
		private E head;

		public HeadStream(MutableObjectIterator<E> iterator, TypeSerializer<E> serializer, TypeComparator<E> comparator)
		throws IOException
		{
			this.iterator = iterator;
			this.comparator = comparator;
			this.head = serializer.createInstance();
			
			if (!nextHead()) {
				throw new IllegalStateException();
			}
		}

		public E getHead() {
			return this.head;
		}

		public boolean nextHead() throws IOException
		{
			if ((this.head = this.iterator.next(this.head)) != null) {
				this.comparator.setReference(this.head);
				return true;
			}
			else {
				return false;
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	
	private static final class HeadStreamComparator<E> implements Comparator<HeadStream<E>>
	{		
		@Override
		public int compare(HeadStream<E> o1, HeadStream<E> o2)
		{
			return o2.comparator.compareToReference(o1.comparator);
		}
	}
}
