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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.util.EmptyIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

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
	
	private static final Log LOG = LogFactory.getLog(SortMergeCoGroupIterator.class);
	
	// --------------------------------------------------------------------------------------------
	
	private MatchStatus matchStatus;
	
	private Iterator<T1> firstReturn;
	
	private Iterator<T2> secondReturn;
	
	private TypePairComparator<T1, T2> comp;
	
	private KeyGroupedIterator<T1> iterator1;

	private KeyGroupedIterator<T2> iterator2;

	private final MutableObjectIterator<T1> reader1;

	private final MutableObjectIterator<T2> reader2;
	
	private final TypeSerializer<T1> serializer1;
	
	private final TypeSerializer<T2> serializer2;
	
	private final TypeComparator<T1> groupingComparator1;
	
	private final TypeComparator<T2> groupingComparator2;
	
	private final TypeComparator<T1> sortingComparator1;
	
	private final TypeComparator<T2> sortingComparator2;
	
	private Sorter<T1> sortMerger1;

	private Sorter<T2> sortMerger2;
	
	private final MemoryManager memoryManager;

	private final IOManager ioManager;
	
	private final LocalStrategy localStrategy;
	
	private final AbstractInvokable parentTask;

	private final long memoryPerChannel;

	private final int fileHandlesPerChannel;
	
	private final float spillingThreshold;

	// --------------------------------------------------------------------------------------------
	
	public SortMergeCoGroupIterator(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<T1> reader1, MutableObjectIterator<T2> reader2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> comparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2,
			TypePairComparator<T1, T2> pairComparator,
			long memory, int maxNumFileHandles, float spillingThreshold,
			LocalStrategy localStrategy, AbstractInvokable parentTask)
	{		
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		
		this.serializer1 = serializer1;
		this.serializer2 = serializer2;
		this.groupingComparator1 = comparator1;
		this.groupingComparator2 = comparator2;
		this.sortingComparator1 = comparator1.duplicate();
		this.sortingComparator2 = comparator2.duplicate();
		this.comp = pairComparator;
		
		this.reader1 = reader1;
		this.reader2 = reader2;
		this.memoryPerChannel = memory / 2;
		this.fileHandlesPerChannel = (maxNumFileHandles / 2) < 2 ? 2 : (maxNumFileHandles / 2);
		this.localStrategy = localStrategy;
		this.parentTask = parentTask;
		this.spillingThreshold = spillingThreshold;
	}
	
	public SortMergeCoGroupIterator(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<T1> reader1, MutableObjectIterator<T2> reader2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> groupingComparator1, TypeComparator<T1> sortingComparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> groupingComparator2, TypeComparator<T2> sortingComparator2,
			TypePairComparator<T1, T2> pairComparator,
			long memory, int maxNumFileHandles, float spillingThreshold,
			LocalStrategy localStrategy, AbstractInvokable parentTask)
	{		
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		
		this.serializer1 = serializer1;
		this.serializer2 = serializer2;
		this.groupingComparator1 = groupingComparator1;
		this.groupingComparator2 = groupingComparator2;
		this.sortingComparator1 = sortingComparator1;
		this.sortingComparator2 = sortingComparator2;
		this.comp = pairComparator;
		
		this.reader1 = reader1;
		this.reader2 = reader2;
		this.memoryPerChannel = memory / 2;
		this.fileHandlesPerChannel = (maxNumFileHandles / 2) < 2 ? 2 : (maxNumFileHandles / 2);
		this.localStrategy = localStrategy;
		this.parentTask = parentTask;
		this.spillingThreshold = spillingThreshold;
	}

	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException
	{
		this.matchStatus = MatchStatus.NONE_REMAINED;	

		// ================================================================
		//                   PERFORMANCE NOTICE
		//
		// It is important to instantiate the sort-mergers both before 
		// obtaining the iterator from one of them. The reason is that
		// the getIterator() method freezes until the first value is
		// available and both sort-mergers should be instantiated and
		// running in the background before this thread waits.
		// ================================================================
		
		if (this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_FIRST_MERGE)
		{
			this.sortMerger1 = new UnilateralSortMerger<T1>(this.memoryManager, this.ioManager,
					this.reader1, this.parentTask, this.serializer1, this.sortingComparator1, 
					this.memoryPerChannel, this.fileHandlesPerChannel, this.spillingThreshold);
		}

		if (this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_SECOND_MERGE)
		{
			this.sortMerger2 = new UnilateralSortMerger<T2>(this.memoryManager, this.ioManager,
				this.reader2, this.parentTask, this.serializer2, this.sortingComparator2, 
				this.memoryPerChannel, this.fileHandlesPerChannel, this.spillingThreshold);
		}
		
		// =============== These calls freeze until the data is actually available ============

		switch (this.localStrategy) {
			case SORT_BOTH_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.sortMerger1.getIterator(), this.serializer1, this.groupingComparator1);
				this.iterator2 = new KeyGroupedIterator<T2>(this.sortMerger2.getIterator(), this.serializer2, this.groupingComparator2);
				break;
			case SORT_FIRST_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.sortMerger1.getIterator(), this.serializer1, this.groupingComparator1);
				this.iterator2 = new KeyGroupedIterator<T2>(this.reader2, this.serializer2, this.groupingComparator2);
				break;
			case SORT_SECOND_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.reader1, this.serializer1, this.groupingComparator1);
				this.iterator2 = new KeyGroupedIterator<T2>(this.sortMerger2.getIterator(), this.serializer2, this.groupingComparator2);
				break;
			case MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.reader1, this.serializer1, this.groupingComparator1);
				this.iterator2 = new KeyGroupedIterator<T2>(this.reader2, this.serializer2, this.groupingComparator2);
				break;
			default:
				throw new RuntimeException("Unsupported Local Strategy in SortMergeCoGroupIterator: " + this.localStrategy);
		}
	}

	@Override
	public void close()
	{
		// close the two sort/merger to release the memory segments
		if (this.sortMerger1 != null) {
			try {
				this.sortMerger1.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing sorter for first input: " + t.getMessage(), t);
			}
		}
		
		if (this.sortMerger2 != null) {
			try {
				this.sortMerger2.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing sorter for second input: " + t.getMessage(), t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator#getValues1()
	 */
	@Override
	public Iterator<T1> getValues1() {
		return this.firstReturn;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator#getValues2()
	 */
	@Override
	public Iterator<T2> getValues2() {
		return this.secondReturn;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator#next()
	 */
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
