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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.plugable.TypeSerializer;
import eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.EmptyIterator;
import eu.stratosphere.pact.runtime.util.KeyComparator;
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
	
	private static enum ReturnStatus {
		RETURN_NONE, RETURN_BOTH, RETURN_FIRST, RETURN_SECOND
	}
	
	private static final Log LOG = LogFactory.getLog(SortMergeCoGroupIterator.class);
	
	// --------------------------------------------------------------------------------------------
	
	private MatchStatus matchStatus;

	private ReturnStatus returnStatus;
	
	private KeyGroupedIterator<T1> iterator1;

	private KeyGroupedIterator<T2> iterator2;

	private final MutableObjectIterator<T1> reader1;

	private final MutableObjectIterator<T2> reader2;
	
	private final TypeSerializer<T1> serializer1;
	
	private final TypeSerializer<T2> serializer2;
	
	private final TypeComparator<T1> comparator1;
	
	private final TypeComparator<T2> comparator2;
	
	private Sorter<T1> sortMerger1;

	private Sorter<T2> sortMerger2;
	
	private final MemoryManager memoryManager;

	private final IOManager ioManager;
	
	private final LocalStrategy localStrategy;
	
	private final AbstractTask parentTask;

	private final long memoryPerChannel;

	private final int fileHandlesPerChannel;
	
	private final float spillingThreshold;

	// --------------------------------------------------------------------------------------------
	
	public SortMergeCoGroupIterator(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<PactRecord> reader1, MutableObjectIterator<PactRecord> reader2,
			int[] firstInputKeyPositions, int[] secondInputKeyPositions, Class<? extends Key>[] keyClasses,
			long memory, int maxNumFileHandles, float spillingThreshold,
			LocalStrategy localStrategy, AbstractTask parentTask)
	{
		if (firstInputKeyPositions.length < 1 || firstInputKeyPositions.length != secondInputKeyPositions.length) {
			throw new IllegalArgumentException("There must be at one, and equally many, key columns for both inputs.");
		}
		
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.keyClasses = keyClasses;
		this.firstKeyPositions = firstInputKeyPositions;
		this.secondKeyPositions = secondInputKeyPositions;
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
		this.returnStatus = ReturnStatus.RETURN_NONE;
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
			// merger
			this.sortMerger1 = new UnilateralSortMerger<T1>(this.memoryManager, this.ioManager,
					this.reader1, this.parentTask, this.serializer1, this.comparator1, 
					this.memoryPerChannel, this.fileHandlesPerChannel, this.spillingThreshold);
		}

		if (this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_SECOND_MERGE)
		{
			// merger
			this.sortMerger2 = new UnilateralSortMerger<T2>(this.memoryManager, this.ioManager,
					this.reader2, this.parentTask, this.serializer2, this.comparator2, 
					this.memoryPerChannel, this.fileHandlesPerChannel, this.spillingThreshold);
		}
		
		// =============== These calls freeze until the data is actually available ============

		switch (this.localStrategy) {
			case SORT_BOTH_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.sortMerger1.getIterator(), this.serializer1, this.comparator1.duplicate());
				this.iterator2 = new KeyGroupedIterator<T2>(this.sortMerger2.getIterator(), this.serializer2, this.comparator2.duplicate());
				break;
			case SORT_FIRST_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.sortMerger1.getIterator(), this.serializer1, this.comparator1.duplicate());
				this.iterator2 = new KeyGroupedIterator<T2>(this.reader2, this.serializer2, this.comparator2.duplicate());
				break;
			case SORT_SECOND_MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.reader1, this.serializer1, this.comparator1.duplicate());
				this.iterator2 = new KeyGroupedIterator<T2>(this.sortMerger2.getIterator(), this.serializer2, this.comparator2.duplicate());
				break;
			case MERGE:
				this.iterator1 = new KeyGroupedIterator<T1>(this.reader1, this.serializer1, this.comparator1.duplicate());
				this.iterator2 = new KeyGroupedIterator<T2>(this.reader2, this.serializer2, this.comparator2.duplicate());
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
				LOG.error("Error closing sort/merger for first input: " + t.getMessage(), t);
			}
		}
		
		if (this.sortMerger2 != null) {
			try {
				this.sortMerger2.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing sort/merger for second input: " + t.getMessage(), t);
			}
		}
	}

	@Override
	public Iterator<T1> getValues1()
	{
		if (this.returnStatus == ReturnStatus.RETURN_SECOND) {
			return EmptyIterator.get();
		} else {
			return this.iterator1.getValues();
		}
	}

	@Override
	public Iterator<T2> getValues2() {
		if (this.returnStatus == ReturnStatus.RETURN_FIRST) {
			return EmptyIterator.get();
		} else {
			return this.iterator2.getValues();
		}
	}

	@Override
	public boolean next() throws IOException {

		Key[] keys1 = null;
		Key[] keys2 = null;

		if (matchStatus != MatchStatus.FIRST_EMPTY) {
			if (matchStatus == MatchStatus.FIRST_REMAINED) {
				keys1 = iterator1.getKeys();
			} else {
				if (iterator1.nextKey()) {
					keys1 = iterator1.getKeys();
				}
			}
		}

		if (matchStatus != MatchStatus.SECOND_EMPTY) {
			if (matchStatus == MatchStatus.SECOND_REMAINED) {
				keys2 = iterator2.getKeys();
			} else {
				if (iterator2.nextKey()) {
					keys2 = iterator2.getKeys();
				}
			}
		}

		if (keys1 == null && keys2 == null) {
			// both inputs are empty
			return false;
		}
		else if (keys1 == null && keys2 != null) {
			// input1 is empty, input2 not
			returnStatus = ReturnStatus.RETURN_SECOND;
			matchStatus = MatchStatus.FIRST_EMPTY;
			return true;
		}
		else if (keys1 != null && keys2 == null) {
			// input1 is not empty, input 2 is empty
			returnStatus = ReturnStatus.RETURN_FIRST;
			matchStatus = MatchStatus.SECOND_EMPTY;
			return true;
		}
		else {
			// both inputs are not empty
			int comp = 0;
			for (int i = 0; i < keys1.length; i++) {
				int c = keys1[i].compareTo(keys2[i]);
				if (c != 0) {
					comp = c;
					break;
				}
			}
			
			if (0 == comp) {
				// keys match
				this.returnStatus = ReturnStatus.RETURN_BOTH;
				this.matchStatus = MatchStatus.NONE_REMAINED;
			}
			else if (0 > comp) {
				// key1 goes first
				this.returnStatus = ReturnStatus.RETURN_FIRST;
				this.matchStatus = MatchStatus.SECOND_REMAINED;
			}
			else {
				// key 2 goes first
				this.returnStatus = ReturnStatus.RETURN_SECOND;
				this.matchStatus = MatchStatus.FIRST_REMAINED;
			}
			return true;
		}
	}
}
