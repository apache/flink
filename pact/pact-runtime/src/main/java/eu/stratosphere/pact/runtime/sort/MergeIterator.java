/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessors;

/**
 * An iterator that returns a sorted merge of the sequences of elements from a
 * set of iterators, assuming those sequences are ordered themselves.
 * The iterators to be merged are kept internally as a heap, making each access
 * to the next smallest element logarithmic in complexity, with respect to the
 * number of streams to be merged.
 * The order among the elements is established using the methods from the
 * {@link TypeAccessors} class, specifically {@link TypeAccessors#setReference(Object)}
 * and {@link TypeAccessors#compareToReference(TypeAccessors)}.
 * 
 * @see TypeAccessors
 * @see TypeAccessors#setReference(Object)
 * @see TypeAccessors#compareToReference(TypeAccessors)
 * 
 * @author Erik Nijkamp
 * @author Stephan Ewen
 */
public class MergeIterator<E> implements MutableObjectIterator<E>
{
	private final PartialOrderPriorityQueue<HeadStream<E>> heap;	// heap over the head elements of the stream
	
	/**
	 * @param iterators
	 * @param accessors The accessors used to establish an order among the elements.
	 *                  The accessors will not be used directly, but a duplicate will be used.
	 * @throws IOException
	 */
	public MergeIterator(List<MutableObjectIterator<E>> iterators,
			TypeAccessors<E> accessors)
	throws IOException
	{
		this.heap = new PartialOrderPriorityQueue<HeadStream<E>>(new HeadStreamComparator<E>(), iterators.size());
		
		for (MutableObjectIterator<E> iterator : iterators) {
			this.heap.add(new HeadStream<E>(iterator, accessors.duplicate()));
		}
	}

	/**
	 * Gets the next smallest element, with respect to the definition of order implied by
	 * the {@link TypeAccessors} provided to this iterator.
	 * 
	 * @param target The object into which the result is put. The contents of the target object
	 *               is only valid after this method, if the method returned true. Otherwise
	 *               the contents is undefined.
	 * @return True, if the iterator had another element, false otherwise. 
	 * 
	 * @see eu.stratosphere.pact.common.util.MutableObjectIterator#next(java.lang.Object)
	 */
	@Override
	public boolean next(E target) throws IOException
	{
		if (this.heap.size() > 0) {
			// get the smallest element
			final HeadStream<E> top = this.heap.peek();
			top.accessors.copyTo(top.getHead(), target);
			
			// read an element
			if (!top.nextHead()) {
				this.heap.poll();
			} else {
				this.heap.adjustTop();
			}
			return true;
		}
		else {
			return false;
		}
	}

	// ============================================================================================
	//                      Internal Classes that wrap the sorted input streams
	// ============================================================================================
	
	private static final class HeadStream<E>
	{
		private final MutableObjectIterator<E> iterator;

		private final TypeAccessors<E> accessors;
		
		private final E head;

		public HeadStream(MutableObjectIterator<E> iterator, TypeAccessors<E> accessors)
		throws IOException
		{
			this.iterator = iterator;
			this.accessors = accessors;
			this.head = accessors.createInstance();
			
			if (!nextHead())
				throw new IllegalStateException();
		}

		public E getHead() {
			return this.head;
		}

		public boolean nextHead() throws IOException
		{
			if (this.iterator.next(this.head)) {
				this.accessors.setReference(this.head);
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
			return o2.accessors.compareToReference(o1.accessors);
		}
	}
}
