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
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;

/**
 * @author Erik Nijkamp
 */
public class SortMergeMatchIterator<K extends Key, V1 extends Value, V2 extends Value> implements
		MatchTaskIterator<K, V1, V2>
{
	private static final Log LOG = LogFactory.getLog(SortMergeMatchIterator.class);
	
	
	private final MemoryManager memoryManager;

	private final IOManager ioManager;

	private final Reader<KeyValuePair<K, V1>> reader1;

	private final Reader<KeyValuePair<K, V2>> reader2;

	private final Class<K> keyClass;

	private final Class<V1> valueClass1;

	private final Class<V2> valueClass2;

	private final long memoryPerChannel;

	private final int fileHandlesPerChannel;

	private KeyValueIterator<V1> iterator1;

	private KeyValueIterator<V2> iterator2;

	private SortMerger<K, V1> sortMerger1;

	private SortMerger<K, V2> sortMerger2;

	private K key;

	private AbstractTask parentTask;



	public SortMergeMatchIterator(MemoryManager memoryManager, IOManager ioManager,
			Reader<KeyValuePair<K, V1>> reader1, Reader<KeyValuePair<K, V2>> reader2,
			Class<K> keyClass, Class<V1> valueClass1, Class<V2> valueClass2,
			long memory, int maxNumFileHandles, AbstractTask parentTask)
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
		this.parentTask = parentTask;
	}

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
		{
			// serialization
			final SerializationFactory<K> keySerialization = new WritableSerializationFactory<K>(keyClass);
			final SerializationFactory<V1> valSerialization = new WritableSerializationFactory<V1>(valueClass1);

			// merger
			this.sortMerger1 = new UnilateralSortMerger<K, V1>(this.memoryManager, this.ioManager,
					this.memoryPerChannel, this.fileHandlesPerChannel, keySerialization,
				valSerialization, keyComparator, this.reader1, this.parentTask);
		}

		{
			// serialization
			final SerializationFactory<K> keySerialization = new WritableSerializationFactory<K>(keyClass);
			final SerializationFactory<V2> valSerialization = new WritableSerializationFactory<V2>(valueClass2);

			// merger
			this.sortMerger2 = new UnilateralSortMerger<K, V2>(this.memoryManager, this.ioManager, 
					this.memoryPerChannel, this.fileHandlesPerChannel, keySerialization,
				valSerialization, keyComparator, this.reader2, this.parentTask);
		}
			
		// =============== These calls freeze until the data is actually available ============ 
		
		this.iterator1 = new KeyValueIterator<V1>(sortMerger1.getIterator());
		this.iterator2 = new KeyValueIterator<V2>(sortMerger2.getIterator());
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
		return iterator1.getValues();
	}

	@Override
	public Iterator<V2> getValues2() {
		return iterator2.getValues();
	}

	@Override
	public boolean next() {
		if (!iterator1.nextKey() || !iterator2.nextKey()) {
			return false;
		}

		K key1 = iterator1.getKey();
		K key2 = iterator2.getKey();

		// zig zag
		while (key1.compareTo(key2) != 0) {
			if (key1.compareTo(key2) > 0) {
				if (!iterator2.nextKey()) {
					return false;
				}
				key2 = iterator2.getKey();
			} else if (key1.compareTo(key2) < 0) {
				if (!iterator1.nextKey()) {
					return false;
				}
				key1 = iterator1.getKey();
			}
		}

		key = key1;

		return true;
	}
	
	private class KeyValueIterator<V extends Value> {
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

		public Iterator<V> getValues() {
			return new Iterator<V>() {
				boolean first = true;

				boolean last = false;

				@Override
				public boolean hasNext() {
					if (first) {
						first = false;
						return true;
					} else if (last) {
						return false;
					} else {
						if (!iterator.hasNext()) {
							return false;
						}

						KeyValuePair<K, V> prev = next;
						next = iterator.next();
						if (next.getKey().compareTo(prev.getKey()) == 0) {
							return true;
						} else {
							last = true;
							nextKey = true;
							return false;
						}
					}
				}

				@Override
				public V next() {
					return next.getValue();
				}

				@Override
				public void remove() {

				}
			};
		}
	};

}
