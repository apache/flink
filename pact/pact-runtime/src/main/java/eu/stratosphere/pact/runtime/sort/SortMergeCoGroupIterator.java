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
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator;
import eu.stratosphere.pact.runtime.task.util.EmptyIterator;
import eu.stratosphere.pact.runtime.task.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * @author Fabian Hueske
 * @author Erik Nijkamp
 */
public class SortMergeCoGroupIterator<K extends Key, V1 extends Value, V2 extends Value> implements
		CoGroupTaskIterator<K, V1, V2>
{
	private static final Log LOG = LogFactory.getLog(SortMergeCoGroupIterator.class);
	
	
	private final MemoryManager memoryManager;

	private final IOManager ioManager;

	private final Reader<KeyValuePair<K, V1>> reader1;

	private final Reader<KeyValuePair<K, V2>> reader2;

	private final Class<K> keyClass;

	private final Class<V1> valueClass1;

	private final Class<V2> valueClass2;

	private final long memoryPerChannel;

	private final int fileHandlesPerChannel;
	
	private final float spillingThreshold;

	private KeyGroupedIterator<K, V1> iterator1;

	private KeyGroupedIterator<K, V2> iterator2;

	private SortMerger<K, V1> sortMerger1;

	private SortMerger<K, V2> sortMerger2;

	private final LocalStrategy localStrategy;
	
	private final AbstractTask parentTask;

	private K key;

	private static enum MatchStatus {
		NONE_REMAINED, FIRST_REMAINED, SECOND_REMAINED, FIRST_EMPTY, SECOND_EMPTY
	}

	private MatchStatus matchStatus;

	private static enum ReturnStatus {
		RETURN_NONE, RETURN_BOTH, RETURN_FIRST, RETURN_SECOND
	}

	private ReturnStatus returnStatus;



	public SortMergeCoGroupIterator(MemoryManager memoryManager, IOManager ioManager,
			Reader<KeyValuePair<K, V1>> reader1, Reader<KeyValuePair<K, V2>> reader2,
			Class<K> keyClass, Class<V1> valueClass1, Class<V2> valueClass2,
			long memory, int maxNumFileHandles, float spillingThreshold,
			LocalStrategy localStrategy, AbstractTask parentTask)
	{
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.keyClass = keyClass;
		this.valueClass1 = valueClass1;
		this.valueClass2 = valueClass2;
		this.reader1 = reader1;
		this.reader2 = reader2;
		this.memoryPerChannel = memory / 2;
		this.fileHandlesPerChannel = (maxNumFileHandles / 2) < 2 ? 2 : (maxNumFileHandles / 2);
		this.localStrategy = localStrategy;
		this.parentTask = parentTask;
		this.spillingThreshold = spillingThreshold;
	}

	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException {
		returnStatus = ReturnStatus.RETURN_NONE;
		matchStatus = MatchStatus.NONE_REMAINED;
		

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
		
		if(this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_FIRST_MERGE)
		{
			// serialization
			final SerializationFactory<K> keySerialization = new WritableSerializationFactory<K>(keyClass);
			final SerializationFactory<V1> valSerialization = new WritableSerializationFactory<V1>(valueClass1);

			// merger
			this.sortMerger1 = new UnilateralSortMerger<K, V1>(this.memoryManager, this.ioManager,
					this.memoryPerChannel, this.fileHandlesPerChannel, keySerialization,
					valSerialization, keyComparator, this.reader1, this.parentTask, this.spillingThreshold);
		}

		if(this.localStrategy == LocalStrategy.SORT_BOTH_MERGE || this.localStrategy == LocalStrategy.SORT_SECOND_MERGE)
		{
			// serialization
			final SerializationFactory<K> keySerialization = new WritableSerializationFactory<K>(keyClass);
			final SerializationFactory<V2> valSerialization = new WritableSerializationFactory<V2>(valueClass2);

			// merger
			this.sortMerger2 = new UnilateralSortMerger<K, V2>(this.memoryManager, this.ioManager, 
					this.memoryPerChannel, this.fileHandlesPerChannel, keySerialization,
					valSerialization, keyComparator, reader2, parentTask, this.spillingThreshold);
		}
		
		// =============== These calls freeze until the data is actually available ============

		switch (this.localStrategy) {
			case SORT_BOTH_MERGE:
				this.iterator1 = new KeyGroupedIterator<K, V1>(sortMerger1.getIterator());
				this.iterator2 = new KeyGroupedIterator<K, V2>(sortMerger2.getIterator());
				break;
			case SORT_FIRST_MERGE:
				this.iterator1 = new KeyGroupedIterator<K, V1>(sortMerger1.getIterator());
				this.iterator2 = new KeyGroupedIterator<K, V2>(new NepheleReaderIterator<KeyValuePair<K,V2>>(reader2));
				break;
			case SORT_SECOND_MERGE:
				this.iterator1 = new KeyGroupedIterator<K, V1>(new NepheleReaderIterator<KeyValuePair<K,V1>>(reader1));
				this.iterator2 = new KeyGroupedIterator<K, V2>(sortMerger2.getIterator());
				break;
			case MERGE:
				this.iterator1 = new KeyGroupedIterator<K, V1>(new NepheleReaderIterator<KeyValuePair<K,V1>>(reader1));
				this.iterator2 = new KeyGroupedIterator<K, V2>(new NepheleReaderIterator<KeyValuePair<K,V2>>(reader2));
				break;
			default:
				throw new RuntimeException("Unsupported Local Strategy in SortMergeCoGroupIterator: "+this.localStrategy);
		}
		
	}

	@Override
	public void close() {
		// close the two sort/merger to release the memory segments
		if (sortMerger1 != null) {
			try {
				sortMerger1.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing sort/merger for first input: " + t.getMessage(), t);
			}
		}
		
		if (sortMerger2 != null) {
			try {
				sortMerger2.close();
			}
			catch (Throwable t) {
				LOG.error("Error closing sort/merger for second input: " + t.getMessage(), t);
			}
		}
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public Iterator<V1> getValues1() {
		if (returnStatus == ReturnStatus.RETURN_SECOND) {
			return EmptyIterator.get();
		} else {
			return iterator1.getValues();
		}
	}

	@Override
	public Iterator<V2> getValues2() {
		if (returnStatus == ReturnStatus.RETURN_FIRST) {
			return EmptyIterator.get();
		} else {
			return iterator2.getValues();
		}
	}

	@Override
	public boolean next() throws IOException {

		K key1 = null;
		K key2 = null;

		if (matchStatus != MatchStatus.FIRST_EMPTY) {
			if (matchStatus == MatchStatus.FIRST_REMAINED) {
				key1 = iterator1.getKey();
			} else {
				if (iterator1.nextKey()) {
					key1 = iterator1.getKey();
				}
			}
		}

		if (matchStatus != MatchStatus.SECOND_EMPTY) {
			if (matchStatus == MatchStatus.SECOND_REMAINED) {
				key2 = iterator2.getKey();
			} else {
				if (iterator2.nextKey()) {
					key2 = iterator2.getKey();
				}
			}
		}

		if (key1 == null && key2 == null) {
			// both inputs are empty
			return false;
		} else if (key1 == null && key2 != null) {
			// input1 is empty, input2 not

			key = key2;
			returnStatus = ReturnStatus.RETURN_SECOND;
			matchStatus = MatchStatus.FIRST_EMPTY;
			return true;
		} else if (key1 != null && key2 == null) {
			// input1 is not empty, input 2 is empty

			key = key1;
			returnStatus = ReturnStatus.RETURN_FIRST;
			matchStatus = MatchStatus.SECOND_EMPTY;
			return true;
		} else {
			// both inputs are not empty

			if (key1.compareTo(key2) == 0) {
				// keys match
				key = key1;
				returnStatus = ReturnStatus.RETURN_BOTH;
				matchStatus = MatchStatus.NONE_REMAINED;
			} else if (key1.compareTo(key2) < 0) {
				// key1 goes first
				key = key1;
				returnStatus = ReturnStatus.RETURN_FIRST;
				matchStatus = MatchStatus.SECOND_REMAINED;
			} else {
				// key 2 goes first
				key = key2;
				returnStatus = ReturnStatus.RETURN_SECOND;
				matchStatus = MatchStatus.FIRST_REMAINED;
			}

			return true;
		}

	}

}
