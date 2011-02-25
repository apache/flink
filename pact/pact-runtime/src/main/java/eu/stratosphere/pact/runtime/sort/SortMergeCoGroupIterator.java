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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator;
import eu.stratosphere.pact.runtime.task.util.EmptyIterator;

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

	private final int numSortBufferPerChannel;

	private final int sizeSortBufferPerChannel;

	private final int ioMemoryPerChannel;

	private final int fileHandlesPerChannel;

	private KeyValueIterator<V1> iterator1;

	private KeyValueIterator<V2> iterator2;

	private SortMerger<K, V1> sortMerger1;

	private SortMerger<K, V2> sortMerger2;

	private AbstractTask parentTask;

	private K key;

	private enum MatchStatus {
		NONE_REMAINED, FIRST_REMAINED, SECOND_REMAINED, FIRST_EMPTY, SECOND_EMPTY
	}

	private MatchStatus matchStatus;

	private enum ReturnStatus {
		RETURN_NONE, RETURN_BOTH, RETURN_FIRST, RETURN_SECOND
	}

	private ReturnStatus returnStatus;

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

	public SortMergeCoGroupIterator(MemoryManager memoryManager, IOManager ioManager,
			Reader<KeyValuePair<K, V1>> reader1, Reader<KeyValuePair<K, V2>> reader2, Class<K> keyClass,
			Class<V1> valueClass1, Class<V2> valueClass2, int numSortBuffer, int sizeSortBuffer, int ioMemory,
			int maxNumFileHandles, AbstractTask parentTask) {
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.keyClass = keyClass;
		this.valueClass1 = valueClass1;
		this.valueClass2 = valueClass2;
		this.reader1 = reader1;
		this.reader2 = reader2;
		this.numSortBufferPerChannel = numSortBuffer / 2;
		this.sizeSortBufferPerChannel = sizeSortBuffer;
		this.ioMemoryPerChannel = ioMemory / 2;
		this.fileHandlesPerChannel = (maxNumFileHandles / 2) == 1 ? 2 : (maxNumFileHandles / 2);
		this.parentTask = parentTask;
	}

	@Override
	public void open() {
		returnStatus = ReturnStatus.RETURN_NONE;
		matchStatus = MatchStatus.NONE_REMAINED;
		try {

			// comparator
			final Comparator<K> keyComparator = new Comparator<K>() {
				@Override
				public int compare(K k1, K k2) {
					return k1.compareTo(k2);
				}
			};

			// iterator 1
			{
				// serialization
				final SerializationFactory<K> keySerialization = new WritableSerializationFactory<K>(keyClass);
				final SerializationFactory<V1> valSerialization = new WritableSerializationFactory<V1>(valueClass1);

				// merger
				sortMerger1 = new UnilateralSortMerger<K, V1>(memoryManager, ioManager, numSortBufferPerChannel,
					sizeSortBufferPerChannel, ioMemoryPerChannel, fileHandlesPerChannel, keySerialization,
					valSerialization, keyComparator, reader1, parentTask);
				// iterator
				iterator1 = new KeyValueIterator<V1>(sortMerger1.getIterator());
			}

			// iterator 2
			{
				// serialization
				final SerializationFactory<K> keySerialization = new WritableSerializationFactory<K>(keyClass);
				final SerializationFactory<V2> valSerialization = new WritableSerializationFactory<V2>(valueClass2);

				// merger
				sortMerger2 = new UnilateralSortMerger<K, V2>(memoryManager, ioManager, numSortBufferPerChannel,
					sizeSortBufferPerChannel, ioMemoryPerChannel, fileHandlesPerChannel, keySerialization,
					valSerialization, keyComparator, reader2, parentTask);

				// iterator
				iterator2 = new KeyValueIterator<V2>(sortMerger2.getIterator());
			}
		} catch (Exception ex) {
			// TODO exception handling sucks (en)
			throw new RuntimeException(ex);
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
	public boolean next() throws IOException, InterruptedException {

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
