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

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

/**
 * @author Erik Nijkamp
 * @author Stephan Ewen
 */
public class MergeIterator implements MutableObjectIterator<PactRecord>
{
	private final PartialOrderPriorityQueue<HeadStream> heap;

	
	public MergeIterator(List<MutableObjectIterator<PactRecord>> iterators, Comparator<Key>[] comparators,
			int[] keyPositions, Class<? extends Key>[] keyClasses)
	throws IOException
	{
		this.heap = new PartialOrderPriorityQueue<HeadStream>(new HeadStreamComparator(comparators), iterators.size());
		
		for (MutableObjectIterator<PactRecord> iterator : iterators) {
			heap.add(new HeadStream(iterator, keyPositions, keyClasses));
		}
	}

	@Override
	public boolean next(PactRecord target) throws IOException
	{
		if (this.heap.size() > 0) {
			// get the smallest element
			HeadStream top = heap.peek();
			top.getHead().copyTo(target);
			
			// read an element
			if (!top.nextHead()) {
				heap.poll();
			}
			heap.adjustTop();
			return true;
		}
		else {
			return false;
		}
	}

	// ============================================================================================
	//                      Internal Classes that wrap the sorted input streams
	// ============================================================================================
	
	private static final class HeadStream
	{
		private final MutableObjectIterator<PactRecord> iterator;

		private final Key[] keyHolders;
		
		private final int[] keyPositions;
		
		private final PactRecord head = new PactRecord();

		public HeadStream(MutableObjectIterator<PactRecord> iterator, int[] keyPositions, Class<? extends Key>[] keyClasses)
		throws IOException
		{
			this.iterator = iterator;
			this.keyPositions = keyPositions;
		
			// instantiate the array that caches the key objects
			this.keyHolders = new Key[keyClasses.length];
			for (int i = 0; i < keyClasses.length; i++) {
				if (keyClasses[i] == null) {
					throw new NullPointerException("Key type " + i + " is null.");
				}
				this.keyHolders[i] = InstantiationUtil.instantiate(keyClasses[i], Key.class);
			}
			
			if (!nextHead())
				throw new IllegalStateException();
		}

		public PactRecord getHead() {
			return this.head;
		}

		public boolean nextHead() throws IOException {
			if (iterator.next(this.head)) {
				this.head.getFieldsInto(this.keyPositions, this.keyHolders);
				return true;
			}
			else {
				return false;
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	
	private final class HeadStreamComparator implements Comparator<HeadStream>
	{
		/**
		 * The comparators providing the comparison for the different key fields.
		 */
		private final Comparator<Key>[] comparators;
		
		public HeadStreamComparator(Comparator<Key>[] comparators) {
			this.comparators = comparators;
		}

		@Override
		public int compare(HeadStream o1, HeadStream o2)
		{
			for (int i = 0; i < this.comparators.length; i++) {
				int val = this.comparators[i].compare(o1.keyHolders[i], o2.keyHolders[i]);
				if (val != 0) {
					return val;
				}
			}
			return 0;
		}
	}
}
