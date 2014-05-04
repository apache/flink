/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.task.util.JoinTaskIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;


/**
 * An implementation of the {@link JoinTaskIterator} that realizes the
 * matching through a sort-merge join strategy.
 */
public class MergeMatchIterator<T1, T2, O> implements JoinTaskIterator<T1, T2, O> {
	
	/**
	 * The log used by this iterator to log messages.
	 */
	private static final Log LOG = LogFactory.getLog(MergeMatchIterator.class);
	
	// --------------------------------------------------------------------------------------------
	
	private TypePairComparator<T1, T2> comp;
	
	private KeyGroupedIterator<T1> iterator1;

	private KeyGroupedIterator<T2> iterator2;
	
	private final TypeSerializer<T1> serializer1;
	
	private final TypeSerializer<T2> serializer2;
	
	private T1 copy1;
	
	private T1 spillHeadCopy;
	
	private T2 copy2;
	
	private T2 blockHeadCopy;
	
	private final BlockResettableIterator<T2> blockIt;				// for N:M cross products with same key
	
	private final List<MemorySegment> memoryForSpillingIterator;
	
	private final MemoryManager memoryManager;

	private final IOManager ioManager;
	
	// --------------------------------------------------------------------------------------------
	
	public MergeMatchIterator(MutableObjectIterator<T1> input1, MutableObjectIterator<T2> input2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> comparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2, TypePairComparator<T1, T2> pairComparator,
			MemoryManager memoryManager, IOManager ioManager, int numMemoryPages, AbstractInvokable parentTask)
	throws MemoryAllocationException
	{
		if (numMemoryPages < 2) {
			throw new IllegalArgumentException("Merger needs at least 2 memory pages.");
		}
		
		this.comp = pairComparator;
		this.serializer1 = serializer1;
		this.serializer2 = serializer2;
		
		this.copy1 = serializer1.createInstance();
		this.spillHeadCopy = serializer1.createInstance();
		this.copy2 = serializer2.createInstance();
		this.blockHeadCopy = serializer2.createInstance();
		
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		
		this.iterator1 = new KeyGroupedIterator<T1>(input1, this.serializer1, comparator1.duplicate());
		this.iterator2 = new KeyGroupedIterator<T2>(input2, this.serializer2, comparator2.duplicate());
		
		final int numPagesForSpiller = numMemoryPages > 20 ? 2 : 1;
		this.blockIt = new BlockResettableIterator<T2>(this.memoryManager, this.serializer2,
			(numMemoryPages - numPagesForSpiller), parentTask);
		this.memoryForSpillingIterator = memoryManager.allocatePages(parentTask, numPagesForSpiller);
	}


	@Override
	public void open() throws IOException {}


	@Override
	public void close() {
		if (this.blockIt != null) {
			try {
				this.blockIt.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing block memory iterator: " + t.getMessage(), t);
			}
		}
		
		this.memoryManager.release(this.memoryForSpillingIterator);
	}
	

	@Override
	public void abort() {
		close();
	}

	/**
	 * Calls the <code>JoinFunction#match()</code> method for all two key-value pairs that share the same key and come 
	 * from different inputs. The output of the <code>match()</code> method is forwarded.
	 * <p>
	 * This method first zig-zags between the two sorted inputs in order to find a common
	 * key, and then calls the match stub with the cross product of the values.
	 * 
	 * @throws Exception Forwards all exceptions from the user code and the I/O system.
	 * 
	 * @see eu.stratosphere.pact.runtime.task.util.JoinTaskIterator#callWithNextKey()
	 */
	@Override
	public boolean callWithNextKey(final GenericJoiner<T1, T2, O> matchFunction, final Collector<O> collector)
	throws Exception
	{
		if (!this.iterator1.nextKey() || !this.iterator2.nextKey()) {
			// consume all remanining keys (hack to prevent remaining inputs during iterations, lets get rid of this soon)
			while (this.iterator1.nextKey());
			while (this.iterator2.nextKey());
			
			return false;
		}

		final TypePairComparator<T1, T2> comparator = this.comp;
		comparator.setReference(this.iterator1.getCurrent());
		T2 current2 = this.iterator2.getCurrent();
				
		// zig zag
		while (true) {
			// determine the relation between the (possibly composite) keys
			final int comp = comparator.compareToReference(current2);
			
			if (comp == 0) {
				break;
			}
			
			if (comp < 0) {
				if (!this.iterator2.nextKey()) {
					return false;
				}
				current2 = this.iterator2.getCurrent();
			}
			else {
				if (!this.iterator1.nextKey()) {
					return false;
				}
				comparator.setReference(this.iterator1.getCurrent());
			}
		}
		
		// here, we have a common key! call the match function with the cross product of the
		// values
		final KeyGroupedIterator<T1>.ValuesIterator values1 = this.iterator1.getValues();
		final KeyGroupedIterator<T2>.ValuesIterator values2 = this.iterator2.getValues();
		
		final T1 firstV1 = values1.next();
		final T2 firstV2 = values2.next();	
			
		final boolean v1HasNext = values1.hasNext();
		final boolean v2HasNext = values2.hasNext();

		// check if one side is already empty
		// this check could be omitted if we put this in MatchTask.
		// then we can derive the local strategy (with build side).
		
		if (v1HasNext) {
			if (v2HasNext) {
				// both sides contain more than one value
				// TODO: Decide which side to spill and which to block!
				crossMwithNValues(firstV1, values1, firstV2, values2, matchFunction, collector);
			} else {
				crossSecond1withNValues(firstV2, firstV1, values1, matchFunction, collector);
			}
		} else {
			if (v2HasNext) {
				crossFirst1withNValues(firstV1, firstV2, values2, matchFunction, collector);
			} else {
				// both sides contain only one value
				matchFunction.join(firstV1, firstV2, collector);
			}
		}
		return true;
	}

	/**
	 * Crosses a single value from the first input with N values, all sharing a common key.
	 * Effectively realizes a <i>1:N</i> match (join).
	 * 
	 * @param val1 The value form the <i>1</i> side.
	 * @param firstValN The first of the values from the <i>N</i> side.
	 * @param valsN Iterator over remaining <i>N</i> side values.
	 *          
	 * @throws Exception Forwards all exceptions thrown by the stub.
	 */
	private void crossFirst1withNValues(final T1 val1, final T2 firstValN,
			final Iterator<T2> valsN, final GenericJoiner<T1, T2, O> matchFunction, final Collector<O> collector)
	throws Exception
	{
		this.copy1 = this.serializer1.copy(val1, this.copy1);
		matchFunction.join(this.copy1, firstValN, collector);
		
		// set copy and match first element
		boolean more = true;
		do {
			final T2 nRec = valsN.next();
			
			if (valsN.hasNext()) {
				this.copy1 = this.serializer1.copy(val1, this.copy1);
				matchFunction.join(this.copy1, nRec, collector);
			} else {
				matchFunction.join(val1, nRec, collector);
				more = false;
			}
		}
		while (more);
	}
	
	/**
	 * Crosses a single value from the second side with N values, all sharing a common key.
	 * Effectively realizes a <i>N:1</i> match (join).
	 * 
	 * @param val1 The value form the <i>1</i> side.
	 * @param firstValN The first of the values from the <i>N</i> side.
	 * @param valsN Iterator over remaining <i>N</i> side values.
	 *          
	 * @throws Exception Forwards all exceptions thrown by the stub.
	 */
	private void crossSecond1withNValues(T2 val1, T1 firstValN,
			Iterator<T1> valsN, GenericJoiner<T1, T2, O> matchFunction, Collector<O> collector)
	throws Exception
	{
		this.copy2 = this.serializer2.copy(val1, this.copy2);
		matchFunction.join(firstValN, this.copy2, collector);
		
		// set copy and match first element
		boolean more = true;
		do {
			final T1 nRec = valsN.next();
			
			if (valsN.hasNext()) {
				this.copy2 = this.serializer2.copy(val1, this.copy2);
				matchFunction.join(nRec, this.copy2, collector);
			} else {
				matchFunction.join(nRec, val1, collector);
				more = false;
			}
		}
		while (more);
	}
	
	/**
	 * @param firstV1
	 * @param spillVals
	 * @param firstV2
	 * @param blockVals
	 */
	private void crossMwithNValues(final T1 firstV1, Iterator<T1> spillVals,
			final T2 firstV2, final Iterator<T2> blockVals,
			final GenericJoiner<T1, T2, O> matchFunction, final Collector<O> collector)
	throws Exception
	{
		// ==================================================
		// We have one first (head) element from both inputs (firstV1 and firstV2)
		// We have an iterator for both inputs.
		// we make the V1 side the spilling side and the V2 side the blocking side.
		// In order to get the full cross product without unnecessary spilling, we do the
		// following:
		// 1) cross the heads
		// 2) cross the head of the spilling side against the first block of the blocking side
		// 3) cross the iterator of the spilling side with the head of the block side
		// 4) cross the iterator of the spilling side with the first block
		// ---------------------------------------------------
		// If the blocking side has more than one block, we really need to make the spilling side fully
		// resettable. For each further block on the block side, we do:
		// 5) cross the head of the spilling side with the next block
		// 6) cross the spilling iterator with the next block.
		
		// match the first values first
		this.copy1 = this.serializer1.copy(firstV1, this.copy1);
		this.blockHeadCopy = this.serializer2.copy(firstV2, this.blockHeadCopy);
		
		// --------------- 1) Cross the heads -------------------
		matchFunction.join(this.copy1, firstV2, collector);
		
		// for the remaining values, we do a block-nested-loops join
		SpillingResettableIterator<T1> spillIt = null;
		
		try {
			// create block iterator on the second input
			this.blockIt.reopen(blockVals);
			
			// ------------- 2) cross the head of the spilling side with the first block ------------------
			while (this.blockIt.hasNext()) {
				final T2 nextBlockRec = this.blockIt.next();
				this.copy1 = this.serializer1.copy(firstV1, this.copy1);
				matchFunction.join(this.copy1, nextBlockRec, collector);
			}
			this.blockIt.reset();
			
			// spilling is required if the blocked input has data beyond the current block.
			// in that case, create the spilling iterator
			final Iterator<T1> leftSideIter;
			final boolean spillingRequired = this.blockIt.hasFurtherInput();
			if (spillingRequired)
			{
				// more data than would fit into one block. we need to wrap the other side in a spilling iterator
				// create spilling iterator on first input
				spillIt = new SpillingResettableIterator<T1>(spillVals, this.serializer1,
						this.memoryManager, this.ioManager, this.memoryForSpillingIterator);
				leftSideIter = spillIt;
				spillIt.open();
				
				this.spillHeadCopy = this.serializer1.copy(firstV1, this.spillHeadCopy);
			}
			else {
				leftSideIter = spillVals;
			}
			
			// cross the values in the v1 iterator against the current block
			
			while (leftSideIter.hasNext()) {
				final T1 nextSpillVal = leftSideIter.next();
				this.copy1 = this.serializer1.copy(nextSpillVal, this.copy1);
				
				
				// -------- 3) cross the iterator of the spilling side with the head of the block side --------
				this.copy2 = this.serializer2.copy(this.blockHeadCopy, this.copy2);
				matchFunction.join(this.copy1, this.copy2, collector);
				
				// -------- 4) cross the iterator of the spilling side with the first block --------
				while (this.blockIt.hasNext()) {
					T2 nextBlockRec = this.blockIt.next();
					
					// get instances of key and block value
					this.copy1 = this.serializer1.copy(nextSpillVal, this.copy1);
					matchFunction.join(this.copy1, nextBlockRec, collector);
				}
				// reset block iterator
				this.blockIt.reset();
			}
			
			// if everything from the block-side fit into a single block, we are done.
			// note that in this special case, we did not create a spilling iterator at all
			if (!spillingRequired) {
				return;
			}
			
			// here we are, because we have more blocks on the block side
			// loop as long as there are blocks from the blocked input
			while (this.blockIt.nextBlock())
			{
				// rewind the spilling iterator
				spillIt.reset();
				
				// ------------- 5) cross the head of the spilling side with the next block ------------
				while (this.blockIt.hasNext()) {
					this.copy1 = this.serializer1.copy(this.spillHeadCopy, this.copy1);
					final T2 nextBlockVal = blockIt.next();
					matchFunction.join(this.copy1, nextBlockVal, collector);
				}
				this.blockIt.reset();
				
				// -------- 6) cross the spilling iterator with the next block. ------------------
				while (spillIt.hasNext())
				{
					// get value from resettable iterator
					final T1 nextSpillVal = spillIt.next();
					// cross value with block values
					while (this.blockIt.hasNext()) {
						// get instances of key and block value
						final T2 nextBlockVal = this.blockIt.next();
						this.copy1 = this.serializer1.copy(nextSpillVal, this.copy1);
						matchFunction.join(this.copy1, nextBlockVal, collector);	
					}
					
					// reset block iterator
					this.blockIt.reset();
				}
				// reset v1 iterator
				spillIt.reset();
			}
		}
		finally {
			if (spillIt != null) {
				this.memoryForSpillingIterator.addAll(spillIt.close());
			}
		}
	}
}
