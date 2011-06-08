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

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.serialization.ValueDeserializer;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.task.util.LastRepeatableIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.RepeatableIteratorWrapper;
import eu.stratosphere.pact.runtime.task.util.SerializationCopier;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;


/**
 * An implementation of the {@link eu.stratosphere.pact.runtime.task.util.MatchTaskIterator} that realizes the
 * matching through a sort-merge join strategy.
 * 
 * @author Erik Nijkamp
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public class SortMergeMatchIterator<K extends Key, V1 extends Value, V2 extends Value>
implements MatchTaskIterator<K, V1, V2>
{
	/**
	 * The log used by this iterator to log messages.
	 */
	private static final Log LOG = LogFactory.getLog(SortMergeMatchIterator.class);
	
	/**
	 * The fraction of the memory that is dedicated to the spilling resettable iterator, which is used in cases where
	 * the cross product of values with the same key becomes very large. 
	 */
	private static final float DEFAULT_MEMORY_SHARE_RATIO = 0.05f;
	
	// --------------------------------------------------------------------------------------------
	
	// utility classes to make deep copies by serializing and de-serializing the data types
	private final SerializationCopier<K> keyCopier = new SerializationCopier<K>();
	private final SerializationCopier<Value> valCopier = new SerializationCopier<Value>();
	
	private final MemoryManager memoryManager;

	private final IOManager ioManager;

	private final Reader<KeyValuePair<K, V1>> reader1;

	private final Reader<KeyValuePair<K, V2>> reader2;
	
	private final SerializationFactory<K> keySerialization;
	
	private final SerializationFactory<V1> value1Serialization;
	
	private final SerializationFactory<V2> value2Serialization;
	
	private final Class<K> keyClass;

	private final Class<V1> value1Class;

	private final Class<V2> value2Class;
	
	private final LocalStrategy localStrategy;
	
	private final AbstractTask parentTask;

	private final long memoryPerChannel;
	
	private final long memoryForBlockNestedLoops;

	private final int fileHandlesPerChannel;
	
	private final float spillingThreshold;

	
	private SortMerger<K, V1> sortMerger1;

	private SortMerger<K, V2> sortMerger2;
	
	private KeyValueIterator<V1> iterator1;

	private KeyValueIterator<V2> iterator2;

	private volatile boolean running = true;

	
	public SortMergeMatchIterator(MemoryManager memoryManager, IOManager ioManager,
			Reader<KeyValuePair<K, V1>> reader1, Reader<KeyValuePair<K, V2>> reader2,
			Class<K> keyClass, Class<V1> value1Class, Class<V2> value2Class,
			long memory, int maxNumFileHandles, float spillingThreshold,
			LocalStrategy localStrategy, AbstractTask parentTask)
	{
		this(memoryManager, ioManager, reader1, reader2, keyClass, value1Class, value2Class, memory, maxNumFileHandles,
			spillingThreshold, DEFAULT_MEMORY_SHARE_RATIO, localStrategy, parentTask);
	}
	
	public SortMergeMatchIterator(MemoryManager memoryManager, IOManager ioManager,
			Reader<KeyValuePair<K, V1>> reader1, Reader<KeyValuePair<K, V2>> reader2,
			Class<K> keyClass, Class<V1> value1Class, Class<V2> value2Class,
			long memory, int maxNumFileHandles, float spillingThreshold, float memPercentageForBlockNL,
			LocalStrategy localStrategy, AbstractTask parentTask)
	{
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		
		this.reader1 = reader1;
		this.reader2 = reader2;
		
		this.keySerialization = new WritableSerializationFactory<K>(keyClass);
		this.value1Serialization = new WritableSerializationFactory<V1>(value1Class);
		this.value2Serialization = new WritableSerializationFactory<V2>(value2Class);
		
		this.keyClass = keyClass;
		this.value1Class = value1Class;
		this.value2Class = value2Class;
		
		this.memoryForBlockNestedLoops = Math.max((long) (memory * memPercentageForBlockNL),
			SpillingResettableIterator.MIN_TOTAL_MEMORY + BlockResettableIterator.MIN_BUFFER_SIZE);
		this.memoryPerChannel = (memory - this.memoryForBlockNestedLoops) / 2;
		this.fileHandlesPerChannel = (maxNumFileHandles / 2) < 2 ? 2 : (maxNumFileHandles / 2);
		this.localStrategy = localStrategy;
		this.parentTask = parentTask;
		this.spillingThreshold = spillingThreshold;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#open()
	 */
	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException
	{
		// comparator
		final Comparator<K> keyComparator = new Comparator<K>() {
			@Override
			public int compare(K k1, K k2) {
				return k1.compareTo(k2);
			}
		};
			
		// ================================================================
		//                   PERFORMANCE NOTICE
		//
		// It is important to instantiate the sort-mergers both before 
		// obtaining the iterator from one of them. The reason is that
		// the getIterator() method freezes until the first value is
		// available and both sort-mergers should be instantiated and
		// running in the background before this thread waits.
		// ================================================================

		// iterator 1
		if(this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_FIRST_MERGE)
		{
			// serialization
			final SerializationFactory<K> keySerialization = new WritableSerializationFactory<K>(this.keyClass);
			final SerializationFactory<V1> valSerialization = new WritableSerializationFactory<V1>(this.value1Class);

			// merger
			this.sortMerger1 = new UnilateralSortMerger<K, V1>(this.memoryManager, this.ioManager,
					this.memoryPerChannel, this.fileHandlesPerChannel, keySerialization,
				valSerialization, keyComparator, this.reader1, this.parentTask, this.spillingThreshold);
		}

		if(this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_SECOND_MERGE)
		{
			// serialization
			final SerializationFactory<K> keySerialization = new WritableSerializationFactory<K>(this.keyClass);
			final SerializationFactory<V2> valSerialization = new WritableSerializationFactory<V2>(this.value2Class);

			// merger
			this.sortMerger2 = new UnilateralSortMerger<K, V2>(this.memoryManager, this.ioManager, 
					this.memoryPerChannel, this.fileHandlesPerChannel, keySerialization,
				valSerialization, keyComparator, this.reader2, this.parentTask, this.spillingThreshold);
		}
			
		// =============== These calls freeze until the data is actually available ============ 
		
		switch (this.localStrategy) {
			case SORT_BOTH_MERGE:
				this.iterator1 = new KeyValueIterator<V1>(this.sortMerger1.getIterator());
				this.iterator2 = new KeyValueIterator<V2>(this.sortMerger2.getIterator());
				break;
			case SORT_FIRST_MERGE:
				this.iterator1 = new KeyValueIterator<V1>(this.sortMerger1.getIterator());
				this.iterator2 = new KeyValueIterator<V2>(new NepheleReaderIterator<KeyValuePair<K,V2>>(this.reader2));
				break;
			case SORT_SECOND_MERGE:
				this.iterator1 = new KeyValueIterator<V1>(new NepheleReaderIterator<KeyValuePair<K,V1>>(this.reader1));
				this.iterator2 = new KeyValueIterator<V2>(this.sortMerger2.getIterator());
				break;
			case MERGE:
				this.iterator1 = new KeyValueIterator<V1>(new NepheleReaderIterator<KeyValuePair<K,V1>>(this.reader1));
				this.iterator2 = new KeyValueIterator<V2>(new NepheleReaderIterator<KeyValuePair<K,V2>>(this.reader2));
				break;
			default:
				throw new RuntimeException("Unsupported Local Strategy in SortMergeMatchIterator: "+this.localStrategy);
		}
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#close()
	 */
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

	/**
	 * Calls the <code>MatchStub#match()</code> method for all two key-value pairs that share the same key and come 
	 * from different inputs. The output of the <code>match()</code> method is forwarded.
	 * <p>
	 * This method first zig-zags between the two sorted inputs in order to find a common
	 * key, and then calls the match stub with the cross product of the values.
	 * 
	 * @throws IOException Thrown, when the reading from the inputs causes an I/O error.
	 * 
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#callWithNextKey()
	 */
	@Override
	public <OK extends Key, OV extends Value> boolean callWithNextKey(MatchStub<K, V1, V2, OK, OV> matchFunction, Collector<OK, OV> collector) throws IOException
	{
		if (!this.iterator1.nextKey() || !this.iterator2.nextKey()) {
			return false;
		}

		K key1 = this.iterator1.getKey();
		K key2 = this.iterator2.getKey();

		// zig zag
		while (key1.compareTo(key2) != 0) {
			if (key1.compareTo(key2) > 0) {
				if (!this.iterator2.nextKey()) {
					return false;
				}
				key2 = this.iterator2.getKey();
			}
			else if (key1.compareTo(key2) < 0) {
				if (!this.iterator1.nextKey()) {
					return false;
				}
				key1 = this.iterator1.getKey();
			}
		}
		
		// here, we have a common key! call the match function with the cross product of the
		// values
		Iterator<V1> values1 = this.iterator1.getValues();
		Iterator<V2> values2 = this.iterator2.getValues();
		
		final V1 firstV1 = values1.next();
		final V2 firstV2 = values2.next();
		
		if (firstV1 == null || firstV2 == null) {
			return false;
		}
			
		final boolean v1HasNext = values1.hasNext();
		final boolean v2HasNext = values2.hasNext();

		// check if one side is already empty
		// this check could be omitted if we put this in MatchTask.
		// then we can derive the local strategy (with build side).
		if (!v1HasNext && !v2HasNext) {
			// both sides contain only one value
			matchFunction.match(key1, firstV1, firstV2, collector);
		}
		else if (!v1HasNext) {
			crossFirst1withNValues(key2, firstV1, firstV2, values2, matchFunction, collector);

		}
		else if (!v2HasNext) {
			crossSecond1withNValues(key1, firstV2, firstV1, values1, matchFunction, collector);
		}
		else {
			// both sides contain more than one value
			// TODO: Decide which side to spill and which to block!
			crossMwithNValues(key1, firstV1, values1, firstV2, values2, matchFunction, collector);
		}
		
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#abort()
	 */
	@Override
	public void abort()
	{
		this.running = false;
	}
	
	// ==============================================================================
	
	/**
	 * Crosses a single value from the first input with N values, all sharing a common key.
	 * Effectively realizes a <i>1:N</i> match (join).
	 * 
	 * @param key The key shared by all values.
	 * @param val1 The value form the <i>1</i> side.
	 * @param firstValN The first of the values from the <i>N</i> side.
	 * @param valsN Iterator over remaining <i>N</i> side values.
	 *          
	 * @throws RuntimeException Forwards all exceptions thrown by the stub.
	 */
	private <OK extends Key, OV extends Value>  void crossFirst1withNValues(
			K key, V1 val1, V2 firstValN, Iterator<V2> valsN,
			MatchStub<K, V1, V2, OK, OV> matchFunction, Collector<OK, OV> collector)
	{
		// set copies
		this.keyCopier.setCopy(key);
		this.valCopier.setCopy(val1);
		
		matchFunction.match(key, val1, firstValN, collector);
		
		// for each of N values
		while (this.running && valsN.hasNext()) {
			// get key copy
			K copiedKey = this.keySerialization.newInstance();
			this.keyCopier.getCopy(copiedKey);
			// get a value copy
			V1 copiedValue = this.value1Serialization.newInstance();
			this.valCopier.getCopy(copiedValue);
		
			// get N value
			V2 vN = valsN.next();
			
			// match
			matchFunction.match(copiedKey, copiedValue, vN, collector);
		}
	}
	
	/**
	 * Crosses a single value from the second side with N values, all sharing a common key.
	 * Effectively realizes a <i>N:1</i> match (join).
	 * 
	 * @param key The key shared by all values.
	 * @param val1 The value form the <i>1</i> side.
	 * @param firstValN The first of the values from the <i>N</i> side.
	 * @param valsN Iterator over remaining <i>N</i> side values.
	 *          
	 * @throws RuntimeException Forwards all exceptions thrown by the stub.
	 */
	private <OK extends Key, OV extends Value> void crossSecond1withNValues(
			K key, V2 val1, V1 firstValN, Iterator<V1> valsN,
			MatchStub<K, V1, V2, OK, OV> matchFunction, Collector<OK, OV> collector)
	throws RuntimeException
	{
		// set copies
		this.keyCopier.setCopy(key);
		this.valCopier.setCopy(val1);
		
		matchFunction.match(key, firstValN, val1, collector);
		
		// for each of N values
		while (this.running && valsN.hasNext()) {
			// get key copy
			K copiedKey = this.keySerialization.newInstance();
			this.keyCopier.getCopy(copiedKey);
			// get a value copy
			V2 copiedValue = this.value2Serialization.newInstance();
			this.valCopier.getCopy(copiedValue);
		
			// get N value
			V1 vN = valsN.next();
			
			// match
			matchFunction.match(copiedKey, vN, copiedValue, collector);
		}
	}
	
	/**
	 * @param key
	 * @param firstV1
	 * @param spillVals
	 * @param firstV2
	 * @param blockVals
	 */
	private <OK extends Key, OV extends Value> void crossMwithNValues(
			final K key, final V1 firstV1, Iterator<V1> spillVals,
			final V2 firstV2, final Iterator<V2> blockVals,
			MatchStub<K, V1, V2, OK, OV> matchFunction, Collector<OK, OV> collector)
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
		this.keyCopier.setCopy(key);
		
		this.valCopier.setCopy(firstV2);
		V2 val2Copy = this.value2Serialization.newInstance();
		this.valCopier.getCopy(val2Copy);
		
		this.valCopier.setCopy(firstV1);
		V1 val1Copy = this.value1Serialization.newInstance();
		this.valCopier.getCopy(val1Copy);
		
		// --------------- 1) Cross the heads -------------------
		matchFunction.match(key, val1Copy, val2Copy, collector);
		
		// for the remaining values, we do a block-nested-loops join
		SpillingResettableIterator<V1> spillIt = null;
		BlockResettableIterator<V2> blockIt = null;
		
		try {
			// create block iterator on the second input
			final ValueDeserializer<V2> v2Deserializer = new ValueDeserializer<V2>(this.value2Class);
			blockIt = new BlockResettableIterator<V2>(this.memoryManager, blockVals, 
					this.memoryForBlockNestedLoops - SpillingResettableIterator.MIN_TOTAL_MEMORY, 1, 
					v2Deserializer, this.parentTask);
			blockIt.open();
			
			// ------------- 2) cross the head of the spilling side with the first block ------------------
			// NOTE: Here we still have the first V1 value in the copier!
			while (blockIt.hasNext()) {
				final K keyCopy = this.keySerialization.newInstance();
				this.keyCopier.getCopy(keyCopy);
				
				val1Copy = this.value1Serialization.newInstance();
				this.valCopier.getCopy(val1Copy);
				
				V2 val2 = blockIt.next();
				
				matchFunction.match(keyCopy, val1Copy, val2, collector);
			}
			blockIt.reset();
			
			// spilling is required if the blocked input has data beyond the current block.
			// in that case, create the spilling iterator
			final LastRepeatableIterator<V1> repeatableIter;
			boolean spillingRequired = blockIt.hasFurtherInput();
			if (spillingRequired)
			{
				// more data than would fit into one block. we need to wrap the other side in a spilling iterator
				// create spilling iterator on first input
				final ValueDeserializer<V1> v1Deserializer = new ValueDeserializer<V1>(this.value1Class);
				spillIt = new SpillingResettableIterator<V1>(this.memoryManager, this.ioManager, spillVals, 
							SpillingResettableIterator.MIN_TOTAL_MEMORY, v1Deserializer, this.parentTask);
				repeatableIter = spillIt;
				
				spillIt.open();
			}
			else {
				repeatableIter = new RepeatableIteratorWrapper<V1>(spillVals, this.value1Serialization);
			}
			
			// cross the values in the v1 iterator against the current block
			this.valCopier.setCopy(firstV2);
			while (repeatableIter.hasNext()) {
				// get value from the spilling side iterator
				V1 nextSpillVal = repeatableIter.next();
				
				// -------- 3) cross the iterator of the spilling side with the head of the block side --------
				K keyCopy = this.keySerialization.newInstance();
				this.keyCopier.getCopy(keyCopy);
				val2Copy = this.value2Serialization.newInstance();
				this.valCopier.getCopy(val2Copy);
				matchFunction.match(keyCopy, nextSpillVal, val2Copy, collector);
				
				// -------- 4) cross the iterator of the spilling side with the first block --------
				while (this.running && blockIt.hasNext()) {
					// get instances of key and block value
					keyCopy = this.keySerialization.newInstance();
					this.keyCopier.getCopy(keyCopy);
					nextSpillVal = repeatableIter.repeatLast();
					final V2 nextBlockVal = blockIt.next();

					matchFunction.match(keyCopy, nextSpillVal, nextBlockVal, collector);						
				}
				// reset block iterator
				blockIt.reset();
			}
			
			// if everything from the block-side fit into a single block, we are done.
			// note that in this special case, we did not create a spilling iterator at all
			if (!spillingRequired) {
				return;
			}
			
			// here we are, because we have more blocks on the block side
			this.valCopier.setCopy(firstV1);
			
			// loop as long as there are blocks from the blocked input
			while (blockIt.nextBlock())
			{
				// rewind the spilling iterator
				spillIt.reset();
				
				// ------------- 5) cross the head of the spilling side with the next block ------------
				while (this.running && blockIt.hasNext()) {
					final K keyCopy = this.keySerialization.newInstance();
					this.keyCopier.getCopy(keyCopy);
					val1Copy = this.value1Serialization.newInstance();
					this.valCopier.getCopy(val1Copy);
					
					final V2 nextBlockVal = blockIt.next();
					matchFunction.match(keyCopy, val1Copy, nextBlockVal, collector);
				}
				blockIt.reset();
				
				// -------- 6) cross the spilling iterator with the next block. ------------------
				while (spillIt.hasNext())
				{
					// get value from resettable iterator
					V1 nextSpillVal = spillIt.next();
					
					// cross value with block values
					while (this.running && blockIt.hasNext()) {
						// get instances of key and block value
						final K keyCopy = this.keySerialization.newInstance();
						this.keyCopier.getCopy(keyCopy);
							
						final V2 nextBlockVal = blockIt.next();

						matchFunction.match(keyCopy, nextSpillVal, nextBlockVal, collector);
							
							// get new instance of resettable value
						if (blockIt.hasNext())
							nextSpillVal = spillIt.repeatLast();
					}
					
					// reset block iterator
					blockIt.reset();
				}
				// reset v1 iterator
				spillIt.reset();
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		finally {
			if (blockIt != null) {
				blockIt.close();
			}
			if (spillIt != null) {
				spillIt.close();
			}
		}
	}
	
	// ==============================================================================
	

	private class KeyValueIterator<V extends Value>
	{
		private boolean nextKey = false;

		private KeyValuePair<K, V> next = null;

		private Iterator<KeyValuePair<K, V>> iterator;

		public KeyValueIterator(Iterator<KeyValuePair<K, V>> iterator) {
			this.iterator = iterator;
		}

		public boolean nextKey() {
			// first pair
			if (next == null) {
				if (iterator.hasNext()) {
					next = iterator.next();
					return true;
				} else {
					return false;
				}
			}

			// known key
			if (nextKey) {
				nextKey = false;
				return true;
			}

			// next key
			while (true) {
				KeyValuePair<K, V> prev = next;
				if (iterator.hasNext()) {
					next = iterator.next();
					if (next.getKey().compareTo(prev.getKey()) != 0) {
						return true;
					}
				} else {
					return false;
				}
			}
		}

		public K getKey() {
			return next.getKey();
		}

		public Iterator<V> getValues()
		{
			return new Iterator<V>() {
				boolean first = true;
				boolean last = false;
				boolean nextCalled = true;

				@Override
				public boolean hasNext() {
					if (first) {
						first = false;
						return true;
					} else if (last) {
						return false;
					} else {
						if(nextCalled) {
							if (!iterator.hasNext()) {
								return false;
							}
							nextCalled = false;
							KeyValuePair<K, V> prev = next;
							next = iterator.next();
							if (next.getKey().compareTo(prev.getKey()) == 0) {
								return true;
							} else {
								last = true;
								nextKey = true;
								return false;
							}
						} else {
							return true;
						}
					}
				}

				@Override
				public V next() {
					if(first) first = false;
					nextCalled = true;
					return next.getValue();
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}
	}
}
