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
import java.util.Iterator;

import eu.stratosphere.api.typeutils.TypeComparator;
import eu.stratosphere.api.typeutils.TypePairComparator;
import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator;
import eu.stratosphere.pact.runtime.util.EmptyIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * @author Fabian Hueske
 * @author Stephan Ewen
 * @author Erik Nijkamp
 */
public class SortMergeCoGroupIterator<T1, T2> implements CoGroupTaskIterator<T1, T2>
{
	private static enum MatchStatus {
		NONE_REMAINED, FIRST_REMAINED, SECOND_REMAINED, FIRST_EMPTY, SECOND_EMPTY
	}
	
	// --------------------------------------------------------------------------------------------
	
	private MatchStatus matchStatus;
	
	private Iterator<T1> firstReturn;
	
	private Iterator<T2> secondReturn;
	
	private TypePairComparator<T1, T2> comp;
	
	private KeyGroupedIterator<T1> iterator1;

	private KeyGroupedIterator<T2> iterator2;

	// --------------------------------------------------------------------------------------------
	
	public SortMergeCoGroupIterator(MutableObjectIterator<T1> input1, MutableObjectIterator<T2> input2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> groupingComparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> groupingComparator2,
			TypePairComparator<T1, T2> pairComparator)
	{		

		this.comp = pairComparator;
		
		this.iterator1 = new KeyGroupedIterator<T1>(input1, serializer1, groupingComparator1);
		this.iterator2 = new KeyGroupedIterator<T2>(input2, serializer2, groupingComparator2);
	}

	@Override
	public void open() {}

	@Override
	public void close() {}


	@Override
	public Iterator<T1> getValues1() {
		return this.firstReturn;
	}


	@Override
	public Iterator<T2> getValues2() {
		return this.secondReturn;
	}


	@Override
	public boolean next() throws IOException
	{
		boolean firstEmpty = true;
		boolean secondEmpty = true;
		
		if (this.matchStatus != MatchStatus.FIRST_EMPTY) {
			if (this.matchStatus == MatchStatus.FIRST_REMAINED) {
				// comparator is still set correctly
				firstEmpty = false;
			} else {
				if (this.iterator1.nextKey()) {
					this.comp.setReference(this.iterator1.getCurrent());
					firstEmpty = false;
				}
			}
		}

		if (this.matchStatus != MatchStatus.SECOND_EMPTY) {
			if (this.matchStatus == MatchStatus.SECOND_REMAINED) {
				secondEmpty = false;
			} else {
				if (iterator2.nextKey()) {
					secondEmpty = false;
				}
			}
		}

		if (firstEmpty && secondEmpty) {
			// both inputs are empty
			return false;
		}
		else if (firstEmpty && !secondEmpty) {
			// input1 is empty, input2 not
			this.firstReturn = EmptyIterator.get();
			this.secondReturn = this.iterator2.getValues();
			this.matchStatus = MatchStatus.FIRST_EMPTY;
			return true;
		}
		else if (!firstEmpty && secondEmpty) {
			// input1 is not empty, input 2 is empty
			this.firstReturn = this.iterator1.getValues();
			this.secondReturn = EmptyIterator.get();
			this.matchStatus = MatchStatus.SECOND_EMPTY;
			return true;
		}
		else {
			// both inputs are not empty
			final int comp = this.comp.compareToReference(this.iterator2.getCurrent());
			
			if (0 == comp) {
				// keys match
				this.firstReturn = this.iterator1.getValues();
				this.secondReturn = this.iterator2.getValues();
				this.matchStatus = MatchStatus.NONE_REMAINED;
			}
			else if (0 < comp) {
				// key1 goes first
				this.firstReturn = this.iterator1.getValues();
				this.secondReturn = EmptyIterator.get();
				this.matchStatus = MatchStatus.SECOND_REMAINED;
			}
			else {
				// key 2 goes first
				this.firstReturn = EmptyIterator.get();
				this.secondReturn = this.iterator2.getValues();
				this.matchStatus = MatchStatus.FIRST_REMAINED;
			}
			return true;
		}
	}
}
